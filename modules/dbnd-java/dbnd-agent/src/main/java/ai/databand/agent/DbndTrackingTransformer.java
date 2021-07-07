package ai.databand.agent;

import ai.databand.config.DbndConfig;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.LoaderClassPath;
import javassist.NotFoundException;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.DuplicateMemberException;
import javassist.bytecode.MethodInfo;
import javassist.expr.ExprEditor;
import javassist.expr.MethodCall;
import javassist.expr.NewExpr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DbndTrackingTransformer implements ClassFileTransformer {

    private static final String TASK_ANNOTATION = "ai.databand.annotations.Task";

    private final DbndConfig config;

    public DbndTrackingTransformer(DbndConfig config) {
        this.config = config;
    }

    public byte[] transform(ClassLoader loader,
                            String className,
                            Class classBeingRedefined,
                            ProtectionDomain protectionDomain,
                            byte[] classfileBuffer) {
        ClassPool cp = ClassPool.getDefault();
        cp.appendClassPath(new LoaderClassPath(loader));
        Optional<CtClass> ctOpt = classInScope(cp, className, classfileBuffer);

        if (!ctOpt.isPresent()) {
            return null;
        }

        try {
            CtClass ct = ctOpt.get();
            System.out.printf("Instrumenting class %s%n", className);
            CtMethod[] declaredMethods = ct.getDeclaredMethods();

            // add $dbnd variable
            try {
                ct.addField(CtField.make("static ai.databand.DbndWrapper $dbnd = ai.databand.DbndWrapper.instance();", ct));
            } catch (DuplicateMemberException e) {
                // do nothing
            }

            for (CtMethod method : declaredMethods) {
                // wrap methods annotated by @task
                MethodInfo methodInfo = method.getMethodInfo();
                AnnotationsAttribute attInfo = (AnnotationsAttribute) methodInfo.getAttribute(AnnotationsAttribute.visibleTag);
                if (attInfo == null) {
                    continue;
                }
                if (attInfo.getAnnotation(TASK_ANNOTATION) == null) {
                    continue;
                }

                CtClass tr = cp.get("java.lang.Throwable");

                if (config.isVerbose()) {
                    System.out.printf("Instrumenting method %s%n", methodInfo.getName());
                }

                injectSparkListener(method);

                method.insertBefore("{ $dbnd.beforeTask(\"" + ct.getName() + "\", \"" + method.getLongName() + "\", $args); }");
                method.insertAfter("{ $dbnd.afterTask(\"" + method.getLongName() + "\", (Object) ($w) $_); }");
                method.addCatch("{ $dbnd.errorTask(\"" + method.getLongName() + "\", $e); throw $e; }", tr);
            }

            return ct.toBytecode();
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("frozen")) {
                return null;
            }
        } catch (Throwable e) {
            System.err.println("Class instrumentation failed");
            e.printStackTrace();
            return null;
        }

        return classfileBuffer;
    }

    protected void injectSparkListener(CtMethod method) throws CannotCompileException {
        if (!config.sparkListenerInjectEnabled()) {
            return;
        }
        final List<Object> listenerAdded = new ArrayList<>(1);

        // detect if spark listener was already added
        method.instrument(
            new ExprEditor() {
                public void edit(NewExpr c) {
                    if ("ai.databand.spark.DbndSparkListener".equalsIgnoreCase(c.getClassName())) {
                        listenerAdded.add(new Object());
                    }
                }
            });

        if (listenerAdded.isEmpty()) {
            // inject spark listener if it wasn't added already
            method.instrument(
                new ExprEditor() {
                    public void edit(MethodCall m) throws CannotCompileException {
                        if (m.getMethodName().contains("getOrCreate") && m.getClassName().equalsIgnoreCase("org.apache.spark.sql.SparkSession$Builder")) {
                            String injected =
                                "{ " +
                                    "$_ = $proceed($$);" +
                                    "$_.sparkContext().addSparkListener(new ai.databand.spark.DbndSparkListener($dbnd));" +
                                    "$_.listenerManager().register(new ai.databand.spark.DbndSparkQueryExecutionListener($dbnd));" +
                                "}";
                            m.replace(injected);
                            if (config.isVerbose()) {
                                System.out.println("Spark listener injected");
                            }
                        }
                    }
                });
        } else {
            if (config.isVerbose()) {
                System.out.println("Spark listener was already added by user, skipped injection");
            }
        }
    }

    protected Optional<CtClass> classInScope(ClassPool cp, String className, byte[] classfileBuffer) {
        try (InputStream is = new ByteArrayInputStream(classfileBuffer)) {
            CtClass ct = cp.makeClass(is);
            CtMethod[] declaredMethods = ct.getDeclaredMethods();
            for (CtMethod method : declaredMethods) {
                MethodInfo methodInfo = method.getMethodInfo();
                AnnotationsAttribute attInfo = (AnnotationsAttribute) methodInfo.getAttribute(AnnotationsAttribute.visibleTag);
                if (attInfo == null) {
                    continue;
                }
                if (attInfo.getAnnotation(TASK_ANNOTATION) != null) {
                    // check if scala object
                    if (!isScalaObject(cp, className)) {
                        return Optional.empty();
                    }
                    return Optional.of(ct);
                }
            }

        } catch (IOException e) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    protected boolean isScalaObject(ClassPool cp, String className) {
        if (className.contains("$")) {
            // this is (probably) scala class
            return true;
        }
        // this can be scala object. Let's check if it has class with '$'
        try {
            cp.get(className + '$');
            // oops! there is actual scala class in classpath
            return false;
        } catch (NotFoundException e) {
            return true;
        }
    }
}
