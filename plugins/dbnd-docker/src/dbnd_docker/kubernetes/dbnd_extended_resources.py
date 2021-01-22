from kubernetes.client.models import V1ResourceRequirements


class DbndExtendedResources(V1ResourceRequirements):
    def __init__(self, requests=None, limits=None, **kwargs):
        self.requests = requests
        self.limits = limits
