from dbnd._core.current import get_databand_context
from dbnd.api.shared_schemas.airflow_monitor import AirflowServerInfoSchema


def list_synced_airflow_instances():
    endpoint = "airflow_monitor"
    schema = AirflowServerInfoSchema(strict=False)
    client = get_databand_context().databand_api_client
    response = client.api_request(endpoint=endpoint, data="", method="GET")
    return schema.load(data=response["data"], many=True).data


def create_airflow_instance(url, external_url, fetcher, api_mode, composer_client_id):
    client = get_databand_context().databand_api_client
    endpoint = "airflow_monitor/add"
    request_data = {
        "base_url": url,
        "ext_url": external_url,
        "fetcher": fetcher,
        "api_mode": api_mode,
        "composer_client_id": composer_client_id,
    }
    client.api_request(endpoint, request_data, method="POST")


def archive_airflow_instance(url):
    client = get_databand_context().databand_api_client
    endpoint = "airflow_monitor/archive"
    client.api_request(endpoint, url, method="POST")


def unarchive_airflow_instance(url):
    client = get_databand_context().databand_api_client
    endpoint = "airflow_monitor/unarchive"
    client.api_request(endpoint, url, method="POST")
