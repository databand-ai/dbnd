import airflow.contrib.kubernetes.pod


class DbndExtendedResources(airflow.contrib.kubernetes.pod.Resources):
    def __init__(self, requests=None, limits=None, **kwargs):
        # py2 support: Resources is not object
        airflow.contrib.kubernetes.pod.Resources.__init__(self, **kwargs)
        self.requests = requests
        self.limits = limits
