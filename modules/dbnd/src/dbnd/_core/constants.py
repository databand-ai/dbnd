# © Copyright Databand.ai, an IBM Company 2022

import datetime
import enum

import attr
import six

from dbnd._core.utils.timezone import make_aware, utcnow


RESULT_PARAM = "result"

CURRENT_DATETIME = utcnow()
CURRENT_TIME_STR = CURRENT_DATETIME.strftime("%Y%m%d_%H%M%S")
CURRENT_DATE = CURRENT_DATETIME.date()


class EnvLabel(object):  # env label
    dev = "dev"
    test = "test"
    staging = "stage"
    qa = "qa"
    prod = "prod"


class CloudType(object):
    local = "local"
    gcp = "gcp"
    aws = "aws"
    azure = "azure"


class TaskExecutorType(object):
    local = "local"


class OutputMode(object):
    regular = "regular"
    prod_immutable = "prod_immutable"


class _ConfigParamContainer(object):
    _type_config = True

    @classmethod
    def is_type_config(self, cls):
        # we can't use issubclass as there are "generic" types that will fail this check
        return getattr(cls, "_type_config", False)


class _TaskParamContainer(object):
    pass


class _TaskDbndRun(object):
    pass


class _TaskDbndRunDataSource(object):
    pass


class DescribeFormat(object):
    short = "short"
    long = "long"
    verbose = "verbose"


# Compute Types


class EnumWithAll(enum.Enum):
    @classmethod
    def all(cls):
        return list(cls)

    @classmethod
    def all_values(cls):
        return [x.value for x in cls]

    @classmethod
    def all_names(cls):
        return [x.name for x in cls]


class SparkClusters(EnumWithAll):
    local = "local"
    dataproc = "dataproc"
    databricks = "databricks"
    emr = "emr"
    qubole = "qubole"


class ApacheBeamClusterType(object):
    local = "local"
    dataflow = "dataflow"


class ClusterPolicy(object):
    NONE = "none"
    CREATE = "create"
    KILL = "kill"
    EPHERMAL = "ephermal"

    ALL = [NONE, CREATE, KILL, EPHERMAL]


class EmrClient(object):
    LIVY = "livy"
    STEP = "step"


class TaskType(object):
    pipeline = "pipeline"
    python = "python"
    spark = "spark"
    pyspark = "pyspark"
    dataflow = "dataflow"
    docker = "docker"


class TaskRunState(EnumWithAll):
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    CANCELLED = "cancelled"
    SHUTDOWN = "shutdown"

    DEFERRED = "deferred"

    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    UP_FOR_RETRY = "up_for_retry"

    REMOVED = "removed"

    @staticmethod
    def final_states():
        return TaskRunState.finished_states() | {
            TaskRunState.UPSTREAM_FAILED,
            TaskRunState.SKIPPED,
            TaskRunState.REMOVED,
        }

    @staticmethod
    def final_states_str():
        return [s.value for s in TaskRunState.final_states()]

    @staticmethod
    def finished_states():
        return {TaskRunState.SUCCESS, TaskRunState.FAILED, TaskRunState.CANCELLED}

    @staticmethod
    def finished_states_str():
        return [s.value for s in TaskRunState.finished_states()]

    @staticmethod
    def direct_fail_states():
        return {TaskRunState.FAILED, TaskRunState.CANCELLED}

    @staticmethod
    def fail_states():
        return {
            TaskRunState.FAILED,
            TaskRunState.CANCELLED,
            TaskRunState.UPSTREAM_FAILED,
        }

    @staticmethod
    def states_lower_case():
        return [state.name.lower() for state in TaskRunState]

    @staticmethod
    def parse(state):
        return TaskRunState(state) if state else None

    def is_after(self, old_state):
        # type: (TaskRunState) -> bool
        # Verify, in case of a race condition that we select the most recent state
        ORDER_MAP = {
            TaskRunState.SCHEDULED: 1,
            TaskRunState.QUEUED: 2,
            TaskRunState.RUNNING: 3,
        }
        new_state_order = ORDER_MAP.get(self, 4)
        old_state_order = ORDER_MAP.get(old_state, 4)

        if new_state_order > old_state_order:
            return True

        # A change in the UI, mark as success or mark as failed
        return self in (TaskRunState.FAILED, TaskRunState.SUCCESS) and self != old_state


REUSED = "reused"


class RunState(EnumWithAll):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SHUTDOWN = "shutdown"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"

    @staticmethod
    def finished_states_str():
        return [
            TaskRunState.SUCCESS.value,
            TaskRunState.FAILED.value,
            TaskRunState.CANCELLED.value,
        ]


class AlertStatus(EnumWithAll):
    TRIGGERED = "TRIGGERED"
    RESOLVED = "RESOLVED"
    ACKNOWLEDGED = "ACKNOWLEDGED"

    @classmethod
    def not_resolved(cls):
        return [cls.TRIGGERED.value, cls.ACKNOWLEDGED.value]


class AlertErrorPolicy(object):
    none = ""
    all = "all"
    task_only = "task_only"


class SystemTaskName(object):
    driver_submit = "dbnd_driver_submit"
    driver = "dbnd_driver"
    task_submit = "dbnd_task_submit"

    driver_and_submitter = {driver_submit, driver}


@attr.s
class _DbndDataClass(object):
    def asdict(self, filter=None):
        return attr.asdict(self, recurse=False, filter=filter)


HEARTBEAT_DISABLED = make_aware(datetime.datetime.fromtimestamp(0))


class ParamValidation(EnumWithAll):
    warn = "warn"
    error = "error"
    disabled = "disabled"


class DbndDatasetOperationType(EnumWithAll):
    read = "read"
    write = "write"
    delete = "delete"


class DbndTargetOperationType(EnumWithAll):
    init = "init"
    read = "read"
    write = "write"
    reuse = "reuse"
    log = "log"
    log_hist = "log_hist"
    delete = "delete"


class DbndTargetOperationStatus(EnumWithAll):
    OK = "OK"
    NOK = "NOK"


class SystemMetrics(EnumWithAll):
    Duration = "Duration"
    TotalDuration = "Total Duration"
    ComputeTime = "Compute Time"
    State = "State"

    @staticmethod
    def duration_metrics():
        """Used to select metrics for removal during metrics re-generation"""
        return [
            s.value
            for s in [
                SystemMetrics.Duration,
                SystemMetrics.TotalDuration,
                SystemMetrics.ComputeTime,
            ]
        ]

    @staticmethod
    def get_description(metric_name):
        if metric_name == SystemMetrics.Duration.value:
            return "Execution time of the latest run attempt of this task"
        elif metric_name == SystemMetrics.TotalDuration.value:
            return "Sum the duration of all task run attempts of this task"
        elif metric_name == SystemMetrics.ComputeTime.value:
            return "Sum of the duration of all task attempts related to this run"
        else:
            return None


class UpdateSource(EnumWithAll):
    # `dbnd` source is only for orchestration
    dbnd = "dbnd"
    airflow_monitor = "airflow_monitor"
    airflow_tracking = "airflow_tracking"
    azkaban_tracking = "azkaban_tracking"
    generic_tracking = "generic_tracking"

    def __eq__(self, other):
        if isinstance(other, UpdateSource):
            return self.value == other.value
        elif isinstance(other, six.string_types):
            return str(self) == other or str(self.value) == other

        return False

    @classmethod
    def is_tracking(cls, source):
        return source in [UpdateSource.airflow_tracking, UpdateSource.azkaban_tracking]


class MetricSource(object):
    user = "user"
    system = "system"
    histograms = "histograms"
    spark = "spark"
    dbt = "dbt"

    @classmethod
    def all(cls):
        return [cls.user, cls.system, cls.histograms, cls.spark]

    @classmethod
    def default_sources(cls):
        return [cls.user, cls.system, cls.histograms, cls.spark]

    @classmethod
    def default_sources_str(cls):
        return ",".join(cls.default_sources())


AD_HOC_DAG_PREFIX = "DBND_RUN."


class AlertSeverity(object):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

    @classmethod
    def values(cls):
        return [cls.CRITICAL, cls.HIGH, cls.MEDIUM, cls.LOW]


class AlertDefOperator(EnumWithAll):
    ANOMALY = "ML"
    RANGE = "range"
    EQUAL = "=="
    NOT_EQUAL = "!="
    GREATER_THAN = ">"
    NOT_LESS_THAN = ">="
    LESS_THAN = "<"
    NOT_GREATER_THAN = "<="


TASK_ESSENCE_ATTR = "task_essence"


class TaskEssence(enum.Enum):
    ORCHESTRATION = "orchestration"
    TRACKING = "tracking"
    CONFIG = "config"

    @classmethod
    def is_task_cls(self, cls):
        return (
            hasattr(cls, TASK_ESSENCE_ATTR)
            and getattr(cls, TASK_ESSENCE_ATTR) != self.CONFIG
        )

    def is_instance(self, obj):
        """
        Checks if the object is include in the essence group.
        >>> TaskEssence.TRACKING.is_instance(obj)
        """
        return (
            hasattr(obj, TASK_ESSENCE_ATTR) and getattr(obj, TASK_ESSENCE_ATTR) == self
        )


class AirflowEnvironment(EnumWithAll):
    ON_PREM = "on_prem"
