/*
 * © Copyright Databand.ai, an IBM Company 2022
 */

package ai.databand;

import ai.databand.config.DbndConfig;
import ai.databand.log.HistogramRequest;
import ai.databand.log.LogDatasetRequest;
import ai.databand.schema.ColumnStats;
import ai.databand.schema.DatabandTaskContext;
import ai.databand.schema.DatasetOperationStatus;
import ai.databand.schema.DatasetOperationType;
import ai.databand.schema.LogDataset;
import ai.databand.schema.TaskRun;
import javassist.ClassPool;
import javassist.Loader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.Dataset;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * AspectJ wrapper for @Pipeline and @Task annonations.
 */
public class DbndWrapper {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DbndWrapper.class);

    private DbndClient dbnd;
    private DbndConfig config;

    // state
    private final Set<String> loadedClasses;
    private final Map<String, Method> methodsCache;
    private DbndRun run;
    private boolean pipelineInitialized;
    private final Deque<String> stack;
    private boolean externalContextSet = false;

    /**
     * Indicates that Spark has started shutdown sequence.
     */
    private boolean isSparkShutdown = false;

    private static final DbndWrapper INSTANCE = new DbndWrapper();

    public static DbndWrapper instance() {
        return INSTANCE;
    }

    public DbndWrapper() {
        initClient();
        methodsCache = new HashMap<>(1);
        stack = new ArrayDeque<>(1);
        loadedClasses = new HashSet<>(1);

        // inject log4j logger appender which will capture all log output and will send it to the tracker.
        String pattern = "[%d] {%c{2}} %p - %m%n";
        DbndLogAppender dbndAppender = new DbndLogAppender(this);
        dbndAppender.setLayout(new PatternLayout(pattern));
        dbndAppender.setThreshold(Level.INFO);
        dbndAppender.activateOptions();

        Logger.getLogger("org.apache.spark").addAppender(dbndAppender);
        Logger.getLogger("org.spark_project").addAppender(dbndAppender);
        Logger.getLogger("ai.databand").addAppender(dbndAppender);
    }

    protected void initClient() {
        config = new DbndConfig();
        try {
            dbnd = new DbndClient(config);
        } catch (Exception e) {
            dbnd = null;
            LOG.error("Unable to initialize DbndClient, tracking will be disabled. Reason: {}", e.getMessage());
            config.setTrackingEnabled(false);
        }
    }

    public Optional<Class<?>> loadClass(String className) {
        try {
            return Optional.of(Class.forName(className));
        } catch (ClassNotFoundException e) {
            // do nothing, class loader we've got doesn't have pipeline class
        }
        try {
            // try to use Javassist classloader
            return Optional.of(new Loader(ClassPool.getDefault()).loadClass(className));
        } catch (ClassNotFoundException e) {
            // do nothing
        }
        return Optional.empty();
    }

    public void beforePipeline(String className, String methodName, Object[] args) {
        Method method = findMethodByName(methodName, className);
        if (method == null) {
            pipelineInitialized = false;
            return;
        }
        // log4j system is not initialized properly at this point, so we're using stdout directly
        System.out.println("Running Databand!");
        System.out.printf("TRACKER URL: %s%n", config.databandUrl());
        System.out.printf("CMD: %s%n", config.cmd());
        System.out.println("Parsed Databand properties: " + config);
        getOrCreateRun(method, args);
        pipelineInitialized = true;
    }

    protected Method findMethodByName(String methodName, String classname) {
        if (classname != null && !loadedClasses.contains(classname)) {
            loadMethods(classname);
        }
        String truncated = removeArgsFromMethodName(methodName);
        for (Map.Entry<String, Method> mthd : methodsCache.entrySet()) {
            if (mthd.getKey().contains(truncated)) {
                return mthd.getValue();
            }
        }
        return null;
    }

    /**
     * Removes arguments part from string representation of method name.
     * ai.databand.JavaSparkPipeline.execute(java.lang.String) → ai.databand.JavaSparkPipeline.execute(
     * Opening parent should be present in result because latter it will be used in methods cache calculation
     *
     * @param methodName
     * @return
     */
    protected String removeArgsFromMethodName(String methodName) {
        int parenIndex = methodName.indexOf("(");
        return parenIndex > 0 ? methodName.substring(0, parenIndex + 1) : methodName;
    }

    protected void loadMethods(String classname) {
        Optional<Class<?>> pipelineClass = loadClass(classname);
        if (!pipelineClass.isPresent()) {
            LOG.error("Unable to build method cache for class {} because it can not be loaded", classname);
            pipelineInitialized = false;
            return;
        }
        for (Method mthd : pipelineClass.get().getDeclaredMethods()) {
            String fullMethodName = mthd.toGenericString();
            methodsCache.put(fullMethodName, mthd);
        }
        loadedClasses.add(classname);
    }

    public void afterPipeline() {
        stop();
        cleanup();
    }

    /**
     * This method is useful when stopping pipelines running from the Databricks Notebooks.
     * Delay is required because in some cases Listener processing will happen after script completion.
     * To handle this, delay has to be added so Listener will process all events and then run will be completed.
     *
     * @param delayInSeconds
     */
    public void afterPipeline(int delayInSeconds) {
        LOG.info("Stopping run with delay");
        try {
            TimeUnit.SECONDS.sleep(delayInSeconds);
        } catch (InterruptedException e) {
            /// do nothing
        }
        stop();
        cleanup();
        LOG.info("Run stopped");
    }

    public void errorPipeline(Throwable error) {
        currentRun().error(error);
        cleanup();
    }

    protected void cleanup() {
        run = null;
        externalContextSet = false;
        methodsCache.clear();
        pipelineInitialized = false;
        loadedClasses.clear();
        initClient();
    }

    public void beforeTask(String className, String methodName, Object[] args) {
        if (!pipelineInitialized) {
            // this is first task, let's initialize pipeline
            if (stack.isEmpty()) {
                beforePipeline(className, methodName, args);
                stack.push(methodName);
            } else {
                // main method was loaded by different classloader
                beforePipeline(className, stack.peek(), args);
            }
            return;
        }
        DbndRun run = currentRun();
        Method method = findMethodByName(methodName, className);
        LOG.info("Running task {}", run.getTaskName(method));
        run.startTask(method, args);
        stack.push(methodName);
    }

    public void afterTask(String methodName, Object result) {
        stack.pop();
        if (stack.isEmpty()) {
            // this was the last task in stack, e.g. pipeline
            afterPipeline();
            return;
        }
        DbndRun run = currentRun();
        Method method = findMethodByName(methodName, null);
        run.completeTask(method, result);
        LOG.info("Task {} has been completed!", run.getTaskName(method));
    }

    public void errorTask(String methodName, Throwable error) {
        String poll = stack.pop();
        LOG.info("Task {} returned error!", poll);
        if (stack.isEmpty()) {
            // this was the last task in stack, e.g. pipeline
            errorPipeline(error);
            return;
        }
        DbndRun run = currentRun();
        Method method = findMethodByName(methodName, null);
        run.errorTask(method, error);
    }

    public void logTask(LoggingEvent event, String eventStr) {
        DbndRun run = currentRun();
        if (run == null) {
            return;
        }
        run.saveLog(event, eventStr);
    }

    public void logMetric(String key, Object value) {
        DbndRun run = currentRun();
        if (run == null) {
            run = createAgentlessRun();
        }
        run.logMetric(key, value);
        LOG.info("Metric logged: [{}: {}]", key, value);
    }

    public void logDatasetOperation(String path,
                                    DatasetOperationType type,
                                    DatasetOperationStatus status,
                                    Dataset<?> data,
                                    Throwable error,
                                    LogDatasetRequest params) {
        DbndRun run = currentRun();
        if (run == null) {
            run = createAgentlessRun();
        }
        run.logDatasetOperation(path, type, status, data, error, params, LogDataset.OP_SOURCE_JAVA_MANUAL_LOGGING);
        LOG.info("Dataset Operation [path: {}], [type: {}], [status: {}] logged", path, type, status);
    }

    public void logDatasetOperation(String path,
                                    DatasetOperationType type,
                                    DatasetOperationStatus status,
                                    String valuePreview,
                                    List<Long> dataDimensions,
                                    String dataSchema,
                                    Boolean withPartition,
                                    List<ColumnStats> columnStats,
                                    String operationSource) {
        DbndRun run = currentRun();
        if (run == null) {
            run = createAgentlessRun();
        }
        run.logDatasetOperation(path, type, status, valuePreview, null, dataDimensions, dataSchema, withPartition, columnStats, operationSource);
        LOG.info("Dataset Operation [path: {}], [type: {}], [status: {}] logged", path, type, status);
        if (isSparkShutdown) {
            // If spark is in the shutdown sequence, pyspark tracking is already completed.
            // This call ensures Spark Listener will send `stop` signal.
            LOG.info("Sending \"SUCCESS\" signal to the task run");
            run.stopListener();
        }
    }

    public void logMetrics(Map<String, Object> metrics) {
        logMetrics(metrics, null);
    }

    public void logMetrics(Map<String, Object> metrics, String source) {
        DbndRun run = currentRun();
        if (run == null) {
            run = createAgentlessRun();
        }
        run.logMetrics(metrics, source);
    }

    public void logDataframe(String key, Dataset<?> value, HistogramRequest histogramRequest) {
        DbndRun run = currentRun();
        if (run == null) {
            run = createAgentlessRun();
        }
        run.logDataframe(key, value, histogramRequest);
    }

    public void logHistogram(Map<String, Object> histogram) {
        DbndRun run = currentRun();
        if (run == null) {
            run = createAgentlessRun();
        }
        run.logHistogram(histogram);
    }

    public void logDataframe(String key, Dataset<?> value, boolean withHistograms) {
        DbndRun run = currentRun();
        if (run == null) {
            run = createAgentlessRun();
        }
        run.logDataframe(key, value, new HistogramRequest(withHistograms));
        LOG.info("Dataframe {} logged", key);
    }

    public void logSpark(SparkListenerEvent event) {
        if (run == null) {
            run = createAgentlessRun();
        }
        if (event instanceof SparkListenerStageCompleted) {
            run.saveSparkMetrics((SparkListenerStageCompleted) event);
            LOG.info("Spark metrics received from SparkListener saved");
        }
    }

    public DbndConfig config() {
        return config;
    }

    // TODO: replace synchronized with better approach to avoid performance bottlenecks
    private synchronized DbndRun getOrCreateRun(Method method, Object[] args) {
        if (currentRun() == null) {
            initRun(method, args);
        }
        return currentRun();
    }

    private DbndRun createAgentlessRun() {
        // add jvm shutdown hook so run will be completed after spark job will stop
        // hook should be added before, because listener is called asynchronously and spark can initialize stop sequence
        if (!config.isTrackingEnabled()) {
            return new NoopDbndRun();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        // check if we're running inside databand task context
        if (config.databandTaskContext().isPresent()) {
            // don't init run from the scratch, reuse values
            run = config.isTrackingEnabled() ? new DefaultDbndRun(dbnd, config) : new NoopDbndRun();
            if (!config.isTrackingEnabled()) {
                System.out.println("Tracking is not enabled. Set DBND__TRACKING variable to True if you want to enable it.");
            }
            System.out.println("Reusing existing task");
            DatabandTaskContext dbndCtx = config.databandTaskContext().get();
            TaskRun driverTask = new TaskRun();
            driverTask.setRunUid(dbndCtx.getRootRunUid());
            driverTask.setTaskRunUid(dbndCtx.getTaskRunUid());
            driverTask.setTaskRunAttemptUid(dbndCtx.getTaskRunAttemptUid());
            config.airflowContext().ifPresent(ctx -> driverTask.setName(ctx.getTaskId()));
            run.setDriverTask(driverTask);
        } else {
            try {
                StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                StackTraceElement main = null;
                for (StackTraceElement el : stackTrace) {
                    if (el.getMethodName().equals("main")) {
                        main = el;
                        break;
                    }
                }
                if (main == null) {
                    main = stackTrace[stackTrace.length - 1];
                }
                // workaround to prevent class not found exception when scala is using layered classloader
                // source: https://github.com/sbt/sbt/issues/4760
                Class<?> entryPoint = Class.forName(main.getClassName(), true, Thread.currentThread().getContextClassLoader());

                for (Method method : entryPoint.getMethods()) {
                    if (method.getName().contains(main.getMethodName())) {
                        Object[] args = new Object[method.getParameterCount()];
                        Arrays.fill(args, null);
                        beforePipeline(main.getClassName(), method.getName(), args);
                        break;
                    }
                }
            } catch (ClassNotFoundException e) {
                System.out.printf("Class not found: %s%n", e.getMessage());
                // do nothing
            }
        }
        if (Objects.isNull(run)) {
            // in case pipeline is not annotated and class not found exception initializing run with no args
            getOrCreateRun(null, null);
        }
        return run;
    }

    protected void stop() {
        if (run != null) { // if run is null it means it's already stopped and cleanup() was called
            run.stop();
        }
    }

    private void setSparkShutdown() {
        isSparkShutdown = true;
    }

    protected DbndRun currentRun() {
        return run;
    }

    private void initRun(Method method, Object[] args) {
        run = config.isTrackingEnabled() ? new DefaultDbndRun(dbnd, config) : new NoopDbndRun();
        if (!config.isTrackingEnabled()) {
            System.out.println("Tracking is not enabled. Set DBND__TRACKING variable to True if you want to enable it.");
            return;
        }
        try {
            run.init(method, args);
            // log4j isn't initialized at this point
            System.out.printf("Running pipeline %s%n", run.getTaskName(method));
        } catch (Exception e) {
            run = new NoopDbndRun();
            System.out.println("Unable to init run:");
            e.printStackTrace();
        }
    }

    protected void printStack() {
        StringBuilder buffer = new StringBuilder(3);
        Iterator<String> iterator = stack.iterator();
        buffer.append('[');
        while (iterator.hasNext()) {
            buffer.append(' ');
            buffer.append(iterator.next());
            buffer.append(' ');
        }
        buffer.append(']');
        LOG.info(buffer.toString());
    }

    /**
     * Set tracking context from external source.
     * This allows us to set context externally (for instance when calling pyspark script) and avoid runs duplication.
     * TODO: since context can be controlled externally in this way, it may sense to start/stop JVM tasks from the Python
     *
     * @param runUid
     * @param taskRunUid
     * @param taskRunAttemptUid
     * @param taskName
     */
    public void setExternalTaskContext(String runUid, String taskRunUid, String taskRunAttemptUid, String taskName) {
        LOG.info("Setting external task context. run_uid: {}, task_run_uid: {}, task_run_attempt_uid: {}, task_name: {}", runUid, taskRunUid, taskRunAttemptUid, taskName);
        // we need to check run_uid before setting external context
        // if it's the same, we'd skip setting
        // if it's different it means that this run was executed against the already running spark
        // and we need to set new context
        if (externalContextSet && runUid.equals(run.getDriverTask().orElse(new TaskRun()).getRunUid())) {
            // external context was already set
            // listener will report all dataset ops to the root task
            // no need to set context again
            LOG.info("Skipping external context setting because it was already set for this run");
            return;
        }
        if (!config.isTrackingEnabled()) {
            run = new NoopDbndRun();
            LOG.info("Attempt to set external task context failed: tracking is not enabled");
            return;
        }
        if (run == null) {
            run = new DefaultDbndRun(dbnd, config);
            // before spark will be stopped we have to submit all saved metrics from the last external task
            Runtime.getRuntime().addShutdownHook(new Thread(run::stopExternal));
            // when pyspark is running, py tracking will complete before Spark will start shutdown sequence
            // Query Listener will still be working during shutdown. We need to know this because listener
            // has to send signal to Databand Tracker to recalculate dataset operations
            Runtime.getRuntime().addShutdownHook(new Thread(this::setSparkShutdown));
        }
        // before setting context we should submit all gathered metrics from a previous context
        run.stopExternal();
        // and then set new context
        TaskRun task = new TaskRun();
        task.setRunUid(runUid);
        task.setTaskRunUid(taskRunUid);
        task.setTaskRunAttemptUid(taskRunAttemptUid);
        task.setName(taskName);
        run.setDriverTask(task);
        externalContextSet = true;
        LOG.info("External task context was set. run_uid: {}, task_run_uid: {}, task_run_attempt_uid: {}, task_name: {}", runUid, taskRunUid, taskRunAttemptUid, taskName);
    }
}


