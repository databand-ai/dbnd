import logging

from abc import ABCMeta

import six


logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class TrackingChannel(object):
    def _handle(self, name, data):
        logger.info("Tracking %s.%s is not implemented", self.__class__.__name__, name)

    def init_scheduled_job(self, data):
        return self._handle(TrackingChannel.init_scheduled_job.__name__, data)

    def init_run(self, data):
        return self._handle(TrackingChannel.init_run.__name__, data)

    def add_task_runs(self, data):
        return self._handle(TrackingChannel.add_task_runs.__name__, data)

    def set_run_state(self, data):
        return self._handle(TrackingChannel.set_run_state.__name__, data)

    def set_task_reused(self, data):
        return self._handle(TrackingChannel.set_task_reused.__name__, data)

    def update_task_run_attempts(self, data):
        return self._handle(TrackingChannel.update_task_run_attempts.__name__, data)

    def set_unfinished_tasks_state(self, data):
        return self._handle(TrackingChannel.set_unfinished_tasks_state.__name__, data)

    def save_task_run_log(self, data):
        return self._handle(TrackingChannel.save_task_run_log.__name__, data)

    def save_external_links(self, data):
        return self._handle(TrackingChannel.save_external_links.__name__, data)

    def log_dataset(self, data):
        return self._handle(TrackingChannel.log_dataset.__name__, data)

    def log_datasets(self, data):
        return self._handle(TrackingChannel.log_datasets.__name__, data)

    def log_target(self, data):
        return self._handle(TrackingChannel.log_target.__name__, data)

    def log_targets(self, data):
        return self._handle(TrackingChannel.log_targets.__name__, data)

    def log_metric(self, data):
        return self._handle(TrackingChannel.log_metric.__name__, data)

    def log_metrics(self, data):
        return self._handle(TrackingChannel.log_metrics.__name__, data)

    def log_artifact(self, data):
        return self._handle(TrackingChannel.log_artifact.__name__, data)

    def heartbeat(self, data):
        return self._handle(TrackingChannel.heartbeat.__name__, data)

    def save_airflow_task_infos(self, data):
        return self._handle(TrackingChannel.save_airflow_task_infos.__name__, data)

    def is_ready(self):
        return self._handle(TrackingChannel.is_ready.__name__, None)

    def get_schema_by_handler_name(self, handler_name):
        raise NotImplementedError()

    def save_airflow_monitor_data(self, data):
        return self._handle(TrackingChannel.save_airflow_monitor_data.__name__, data)
