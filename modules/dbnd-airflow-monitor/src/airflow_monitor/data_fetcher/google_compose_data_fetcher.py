from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.data_fetcher.web_data_fetcher import WebFetcher


class GoogleComposerFetcher(WebFetcher):
    # requires GOOGLE_APPLICATION_CREDENTIALS env variable
    def __init__(self, config):
        # type: (AirflowServerConfig) -> None
        super(GoogleComposerFetcher, self).__init__(config)
        self.client_id = config.composer_client_id
        self.env = "GoogleCloudComposer"

    def _do_make_request(self, endpoint_name, params, timeout):
        from airflow_monitor.make_iap_request import make_iap_request

        resp = make_iap_request(
            url=self.endpoint_url + "/" + endpoint_name.strip("/"),
            client_id=self.client_id,
            params=params,
            timeout=timeout,
        )
        return resp
