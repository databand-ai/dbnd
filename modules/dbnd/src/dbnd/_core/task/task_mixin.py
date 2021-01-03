from abc import abstractmethod

from targets.value_meta import ValueMetaConf


class _TaskCtrlMixin(object):
    @property
    @abstractmethod
    def tracker(self):
        """
        Abstract property for each task to describe how to access it's tracker.
        """
        raise NotImplementedError()

    def log_dataframe(
        self,
        key,
        df,
        with_preview=True,
        with_schema=True,
        with_size=True,
        with_stats=False,
    ):
        meta_conf = ValueMetaConf(
            log_preview=with_preview,
            log_schema=with_schema,
            log_size=with_size,
            log_stats=with_stats,
        )
        self.tracker.log_data(key, df, meta_conf=meta_conf)

    def log_metric(self, key, value, source=None):
        """
        Logs the passed-in parameter under the current run, creating a run if necessary.
        :param key: Parameter name (string)
        :param value: Parameter value (string)
        """
        return self.tracker.log_metric(key, value, source=source)

    def log_system_metric(self, key, value):
        """Shortcut for log_metric(..., source="system") """
        return self.log_metric(key, value, source="system")

    def log_artifact(self, name, artifact):
        """Log a local file or directory as an artifact of the currently active run."""
        return self.tracker.log_artifact(name, artifact)

    @property
    @abstractmethod
    def task_dag(self):
        """
        Abstract property for each task to describe how to access it's task dag controller.
        """
        raise NotImplementedError()

    def set_upstream(self, task_or_task_list):
        self.task_dag.set_upstream(task_or_task_list)

    def set_downstream(self, task_or_task_list):
        self.task_dag.set_downstream(task_or_task_list)

    def __lshift__(self, other):
        return self.set_upstream(other)

    def __rshift__(self, other):
        return self.set_downstream(other)

    def set_global_upstream(self, task_or_task_list):
        self.task_dag.set_global_upstream(task_or_task_list)

    @property
    @abstractmethod
    def descendants(self):
        raise NotImplementedError()
