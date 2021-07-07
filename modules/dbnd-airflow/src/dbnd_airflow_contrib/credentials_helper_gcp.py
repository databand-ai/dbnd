import logging

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


logger = logging.getLogger(__name__)


class GSCredentials(GoogleCloudBaseHook):
    def __init__(self, gcp_conn_id="google_cloud_default", delegate_to=None):
        super(GSCredentials, self).__init__(gcp_conn_id, delegate_to)

    def get_credentials(self):
        try:
            return self._get_credentials()
        except (Exception) as e:
            logger.exception(
                "Failed to load GCP credentials using connection id - "
                + self.gcp_conn_id
            )
            raise
