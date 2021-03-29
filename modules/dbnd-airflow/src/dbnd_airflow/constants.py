from distutils.version import LooseVersion

import airflow


AIRFLOW_VERSION_2 = LooseVersion(airflow.version.version) >= LooseVersion("2.0.0")
AIRFLOW_VERSION_1 = LooseVersion(airflow.version.version) < LooseVersion("2.0.0")

AIRFLOW_ABOVE_10 = LooseVersion(airflow.version.version) > LooseVersion("1.10.10")
AIRFLOW_ABOVE_9 = LooseVersion(airflow.version.version) > LooseVersion("1.10.9")
