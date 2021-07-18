from airflow.contrib.kubernetes.pod import Resources


class DbndExtendedResources(Resources):
    def __init__(self, requests=None, limits=None, **kwargs):
        # py2 support: Resources is not object
        Resources.__init__(self, **kwargs)
        self.requests = requests
        self.limits = limits
