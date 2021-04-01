from dbnd import log_metrics


class SparkListener(object):
    def onApplicationEnd(self, applicationEnd):
        pass

    def onApplicationStart(self, applicationStart):
        pass

    def onBlockManagerRemoved(self, blockManagerRemoved):
        pass

    def onBlockUpdated(self, blockUpdated):
        pass

    def onEnvironmentUpdate(self, environmentUpdate):
        pass

    def onExecutorAdded(self, executorAdded):
        pass

    def onExecutorMetricsUpdate(self, executorMetricsUpdate):
        pass

    def onExecutorRemoved(self, executorRemoved):
        pass

    def onJobEnd(self, jobEnd):
        pass

    def onJobStart(self, jobStart):
        pass

    def onOtherEvent(self, event):
        pass

    def onStageCompleted(self, stageCompleted):
        pass

    def onStageSubmitted(self, stageSubmitted):
        pass

    def onTaskEnd(self, taskEnd):
        pass

    def onTaskGettingResult(self, taskGettingResult):
        pass

    def onTaskStart(self, taskStart):
        pass

    def onUnpersistRDD(self, unpersistRDD):
        pass

    class Java:
        implements = ["org.apache.spark.scheduler.SparkListenerInterface"]


class DbndSparkListener(SparkListener):
    def toString(self):
        return DbndSparkListener.__name__

    def onStageCompleted(self, stageCompleted):
        stage_info = stageCompleted.stageInfo()
        transformation_name = stage_info.name()[0 : stage_info.name().index(" ")]
        metric_prefix = "stage-{}.{}".format(stage_info.stageId(), transformation_name)
        it = stage_info.taskMetrics().accumulators().iterator()
        metrics = {}
        while it.hasNext():
            next_metric = it.next()
            key = "{}.{}".format(metric_prefix, next_metric.name().get())
            value = next_metric.value()
            metrics[key] = value
        log_metrics(metrics, "spark")
