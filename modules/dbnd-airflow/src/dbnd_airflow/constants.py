from distutils.version import LooseVersion

import airflow


AIRFLOW_VERSION_2 = LooseVersion(airflow.version.version) >= LooseVersion("2.0.0")
AIRFLOW_VERSION_1 = LooseVersion(airflow.version.version) < LooseVersion("2.0.0")

AIRFLOW_ABOVE_13 = LooseVersion(airflow.version.version) > LooseVersion("1.10.13")
AIRFLOW_ABOVE_10 = LooseVersion(airflow.version.version) > LooseVersion("1.10.10")
AIRFLOW_ABOVE_9 = LooseVersion(airflow.version.version) > LooseVersion("1.10.9")
AIRFLOW_ABOVE_6 = LooseVersion(airflow.version.version) > LooseVersion("1.10.6")

AIRFLOW_BELOW_10 = LooseVersion(airflow.version.version) < LooseVersion("1.10.10")
