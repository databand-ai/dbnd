import json

from dbnd.api.api_utils import ApiClient


class KillApiClient(object):
    def __init__(self, api_base_url=None, auth=True):
        from dbnd import config

        if not api_base_url:
            api_base_url = config.get("core", "databand_url")
        self.client = ApiClient(api_base_url=api_base_url, auth=auth)

    def kill_run(self, run_uid):
        self.client.api_request(endpoint="runs/stop", data=[run_uid])
