# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.shared.utils import TrackingServiceConfig


logger = logging.getLogger(__name__)


# Do not change this messages without changing the same messages in airflow_monitor_service.py in dbnd-web
AUTO_TRACKING_PACKAGE_IS_MISSING_MESSAGE = "Airflow monitoring is not working due to dbnd-airflow-auto-tracking package not being installed. Run the following command to install the required dbnd PyPI packages on your Airflow scheduler: pip install dbnd-airflow-auto-tracking."
MISSING_AIRFLOW_2_TRACKING_SUPPORT_CONFIG_MESSAGE = "SDK tracking may not be working properly due to lazy_load_plugins config not being to false in Airflow 2.0. You need to disable Lazy Load plugins. This can be done by setting the config in Airflow: core.lazy_load_plugins=False or the environment variable AIRFLOW__CORE__LAZY_LOAD_PLUGINS=False"


class ValidationStep:
    def __init__(self):
        self.errors_list = []

    def run_validation(self):
        raise NotImplemented()


class CheckDbndConfig(ValidationStep):
    def run_validation(self):
        monitor_config = AirflowMonitorConfig.from_env()
        tracking_config = TrackingServiceConfig.from_env()

        if not tracking_config.url:
            self.errors_list.append("No databand url found in the configuration")

        if not tracking_config.access_token:
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
            from dbnd_airflow.compat import AIRFLOW_VERSION_1
        except:
            logger.warning("No Airflow was found/airflow version can not be calculated")
            return

        if AIRFLOW_VERSION_1:
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
