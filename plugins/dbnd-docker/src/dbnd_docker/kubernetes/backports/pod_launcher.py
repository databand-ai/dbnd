from distutils.version import LooseVersion

import airflow


if LooseVersion(airflow.version.version) > LooseVersion("1.10.10"):
    from airflow.kubernetes.pod_launcher import PodStatus as PodStatus
else:
    from airflow.contrib.kubernetes.pod_launcher import PodStatus as PodStatus
