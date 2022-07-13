import logging

from datetime import datetime
from functools import wraps

from mlflow.entities import ViewType
from mlflow.store.tracking.abstract_store import AbstractStore

from dbnd._core.task_build.task_context import try_get_current_task
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


def duplication_store(dbnd_store_method):
    @wraps(dbnd_store_method)
    def wrapper(self, *args, **kwargs):
        result = dbnd_store_method(self, *args, **kwargs)
        if self.duplication_store:
            duplication_store_method = getattr(
                self.duplication_store, dbnd_store_method.__name__
            )
            result = duplication_store_method(*args, **kwargs)
        return result

    return wrapper


class DatabandStore(AbstractStore):
    def __init__(self, dbnd_store, duplication_store):
        self.dbnd_store = dbnd_store
        self.duplication_store = duplication_store

    ## Experiments CRUD:
    @duplication_store
    def create_experiment(self, name, artifact_location):
        pass

    @duplication_store
    def list_experiments(self, view_type=ViewType.ACTIVE_ONLY):
        pass

    @duplication_store
    def get_experiment(self, experiment_id):
        pass

    @duplication_store
    def get_experiment_by_name(self, experiment_name):
        pass

    @duplication_store
    def delete_experiment(self, experiment_id):
        pass

    @duplication_store
    def restore_experiment(self, experiment_id):
        pass

    @duplication_store
    def rename_experiment(self, experiment_id, new_name):
        pass

    ## Runs CRUD:

    @duplication_store
    def create_run(self, experiment_id, user_id, start_time, tags):
        pass

    @duplication_store
    def get_run(self, run_id):
        pass

    @duplication_store
    def get_metric_history(self, run_id, metric_key):
        pass

    @duplication_store
    def search_runs(self, *args, **kwargs):
        pass

    @duplication_store
    def _search_runs(self, *args, **kwargs):
        pass

    @duplication_store
    def list_run_infos(self, experiment_id, run_view_type):
        pass

    @duplication_store
    def update_run_info(self, run_id, run_status, end_time):
        pass

    @duplication_store
    def delete_run(self, run_id):
        pass

    @duplication_store
    def restore_run(self, run_id):
        pass

    ## Metadata:

    @duplication_store
    def set_experiment_tag(self, experiment_id, tag):
        pass

    def _get_current_task_run(self):
        task = try_get_current_task()
        if task is None:
            # TODO: fake task
            raise NotImplementedError(
                "DatabandStore usage outside of DBND task is not implemented yet."
            )
        return task.current_task_run

    def _log_metric(self, run_id, metric):
        # type: (str, mlflow.entities.Metric) -> None
        dbnd_metric = Metric(
            key=metric.key,
            value=metric.value,
            # mlflow.entities.Metric.timestamp is `int(time.time() * 1000)`
            timestamp=datetime.fromtimestamp(metric.timestamp / 1000),
        )
        self.dbnd_store.log_metrics(
            task_run=self._get_current_task_run(), metrics=[dbnd_metric]
        )
        logger.info("Metric {}".format(metric))

    def _log_param(self, run_id, param):
        # type: (str, mlflow.entities.Param) -> None
        # Temporarly log params as metrics
        dbnd_metric = Metric(key=param.key, value=param.value, timestamp=utcnow())
        self.dbnd_store.log_metrics(
            task_run=self._get_current_task_run(), metrics=[dbnd_metric]
        )
        logger.info("Param {}".format(param))

    @duplication_store
    def log_metric(self, run_id, metric):
        self._log_metric(run_id, metric)

    @duplication_store
    def log_param(self, run_id, param):
        self._log_param(run_id, param)

    @duplication_store
    def set_tag(self, run_id, tag):
        pass

    @duplication_store
    def log_batch(self, run_id, metrics, params, tags):
        for metric in metrics:
            self._log_metric(run_id, metric)
        for param in params:
            self._log_param(run_id, param)

    @duplication_store
    def record_logged_model(self, run_id, mlflow_model):
        pass
