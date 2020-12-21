from distutils.version import LooseVersion

import airflow


AIRFLOW_ABOVE_10 = LooseVersion(airflow.version.version) > LooseVersion("1.10.10")
AIRFLOW_ABOVE_9 = LooseVersion(airflow.version.version) > LooseVersion("1.10.9")
