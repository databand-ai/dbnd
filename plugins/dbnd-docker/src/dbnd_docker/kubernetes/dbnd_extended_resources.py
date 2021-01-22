from kubernetes.client.models import V1ResourceRequirements


class DbndExtendedResources(V1ResourceRequirements):
    def __init__(self, requests=None, limits=None, **kwargs):
        # py2 support: Resources is not object
        # V1ResourceRequirements.__init__(self, **kwargs)
        self.requests = requests
        self.limits = limits
