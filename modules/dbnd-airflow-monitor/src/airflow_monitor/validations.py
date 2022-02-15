import logging

from distutils.version import LooseVersion

from airflow_monitor.config import AirflowMonitorConfig
from dbnd._core.settings import CoreConfig


logger = logging.getLogger(__name__)


# Do not change this messages without changing the same messages in airflow_monitor_service.py in dbnd-web
AUTO_TRACKING_PACKAGE_IS_MISSING_MESSAGE = (
    "dbnd-airflow-auto-tracking package is not installed"
)
MISSING_AIRFLOW_2_TRACKING_SUPPORT_CONFIG_MESSAGE = (
    "lazy_load_plugins config is not set to false in Airflow 2.0"
)


class ValidationStep:
    def __init__(self):
        self.errors_list = []

    def run_validation(self):
        raise NotImplemented()


class CheckDbndConfig(ValidationStep):
    def run_validation(self):
        monitor_config = AirflowMonitorConfig()
        core_config = CoreConfig()

        if not core_config.databand_url:
            self.errors_list.append("No databand url found in the configuration")

        if not core_config.databand_access_token:
            self.errors_list.append("No access token found in the configuration")

        if not monitor_config.syncer_name:
            self.errors_list.append("No syncer name found in the configuration")

        if len(self.errors_list) == 0:
            logger.info("All required configurations exist")


class CheckMonitorPackages(ValidationStep):
    def run_validation(self):
        # We don't need to validate all packages, since if dbnd-airflow-monitor does not exist,
        # the dag would be broken in Airflow
        try:
            import dbnd_airflow  # noqa: F401
        except ImportError:
            self.errors_list.append(
                "Did not find dbnd-airflow package required for monitor"
            )

        if len(self.errors_list) == 0:
            logger.info("All dbnd packages required for monitor exist")


class CheckTrackingPackages(ValidationStep):
    def run_validation(self):
        try:
            import dbnd_airflow_auto_tracking  # noqa: F401
        except ImportError:
            self.errors_list.append(AUTO_TRACKING_PACKAGE_IS_MISSING_MESSAGE)

        if len(self.errors_list) == 0:
            logger.info("All dbnd packages required for tracking exist")


class CheckAirflow2TrackingSupport(ValidationStep):
    def run_validation(self):
        try:
            import airflow
        except:
            logger.warning("No Airflow was found")
            return

        if LooseVersion(airflow.version.version) < LooseVersion("2.0.0"):
            return

        load_plugins = True
        from airflow.configuration import conf

        try:
            load_plugins = conf.getboolean("core", "lazy_load_plugins")
        except:
            pass

        # lazy_load plugins must be set to false in order for tracking to work
        if load_plugins is None or load_plugins is True:
            self.errors_list.append(MISSING_AIRFLOW_2_TRACKING_SUPPORT_CONFIG_MESSAGE)
        else:
            logger.info("Airflow 2.0 support is set")


def get_monitor_validation_steps():
    return [CheckDbndConfig(), CheckMonitorPackages()]


def get_tracking_validation_steps():
    return [CheckTrackingPackages(), CheckAirflow2TrackingSupport()]


def get_all_errors(validation_steps):
    errors_list = []

    for validation_step in validation_steps:
        validation_step.run_validation()
        errors_list.extend(validation_step.errors_list)

    return errors_list


def run_validations():
    logger.info("Running validations")

    validation_steps = [
        *get_monitor_validation_steps(),
        *get_tracking_validation_steps(),
    ]

    errors_list = get_all_errors(validation_steps)

    if len(errors_list) != 0:
        logger.warning("The following problems found:")
        for error_message in errors_list:
            logger.warning(error_message)
