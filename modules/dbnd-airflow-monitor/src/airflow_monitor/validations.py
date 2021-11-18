import logging

from distutils.version import LooseVersion

from airflow_monitor.config import AirflowMonitorConfig
from dbnd._core.settings import CoreConfig


logger = logging.getLogger(__name__)


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
            self.errors_list.append("No syncer name found in the configurration")

        if len(self.errors_list) == 0:
            logger.info("All required configurations exist")


class CheckPackages(ValidationStep):
    def run_validation(self):
        try:
            import dbnd_airflow
        except ImportError:
            self.errors_list.append(
                "Did not find dbnd-airflow package required for monitor"
            )

        try:
            import dbnd_airflow_auto_tracking
        except ImportError:
            self.errors_list.append(
                "Did not find dbnd-airflow-auto-tracking required for tracking"
            )

        if len(self.errors_list) == 0:
            logger.info("All dbnd packages required for monitor exist")


class CheckAirflow2Support(ValidationStep):
    def run_validation(self):
        try:
            import airflow
        except:
            logger.warning("No Airflow was found")
            return

        if LooseVersion(airflow.version.version) < LooseVersion("2.0.0"):
            return

        load_plugins = False
        from airflow.configuration import conf

        try:
            load_plugins = conf.getboolean("core", "lazy_load_plugins")
            logger.info("Airflow 2.0 support is set")
        except:
            pass

        if not load_plugins:
            self.errors_list.append(
                "AIRFLOW__CORE__LAZY_LOAD_PLUGINS config should be set to false in Airflow 2.0"
            )


def run_validations():
    logger.info("Running validations")

    # We don't need package validation, since if dbnd-airflow-monitor does not exist, the dag would be broken in Airflow
    errors_list = []

    for validation_step in [CheckDbndConfig(), CheckPackages(), CheckAirflow2Support()]:
        validation_step.run_validation()
        errors_list.extend(validation_step.errors_list)

    if len(errors_list) != 0:
        logger.warning("The following problems found:")
        for error_message in errors_list:
            logger.warning(error_message)
