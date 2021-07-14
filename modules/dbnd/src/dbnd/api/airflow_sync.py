from dbnd._core.current import get_databand_context
from dbnd.api.shared_schemas.airflow_monitor import AirflowServerInfoSchema


def list_synced_airflow_instances():
    endpoint = "airflow_monitor"
    schema = AirflowServerInfoSchema(strict=False)
    client = get_databand_context().databand_api_client
    response = client.api_request(endpoint=endpoint, data="", method="GET")
    return schema.load(data=response["data"], many=True).data


def create_airflow_instance(
    url,
    external_url,
    fetcher,
    api_mode,
    composer_client_id,
    fetch_quantity,
    oldest_incomplete_data_in_days,
    include_logs,
    include_task_args,
    include_xcom,
    include_sources,
    dag_ids,
    last_seen_dag_run_id,
    last_seen_log_id,
    use_af_monitor_v2,
):
    client = get_databand_context().databand_api_client
    endpoint = "airflow_monitor/add"
    request_data = {
        "base_url": url,
        "external_url": external_url,
        "fetcher": fetcher,
        "api_mode": api_mode,
        "composer_client_id": composer_client_id,
        "fetch_quantity": fetch_quantity,
        "oldest_incomplete_data_in_days": oldest_incomplete_data_in_days,
        "include_logs": include_logs,
        "include_task_args": include_task_args,
        "include_xcom": include_xcom,
        "include_sources": include_sources,
        "dag_ids": dag_ids,
        "last_seen_dag_run_id": last_seen_dag_run_id,
        "last_seen_log_id": last_seen_log_id,
        "is_sync_enabled_v2": use_af_monitor_v2,
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
