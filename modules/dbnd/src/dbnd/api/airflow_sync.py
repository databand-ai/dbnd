from dbnd._core.current import get_databand_context
from dbnd.api.shared_schemas.airflow_monitor import AirflowServerInfoSchema


def list_synced_airflow_instances():
    endpoint = "airflow_monitor"
    schema = AirflowServerInfoSchema(strict=False)
    client = get_databand_context().databand_api_client
    response = client.api_request(endpoint=endpoint, data="", method="GET")
    return schema.load(data=response["data"], many=True).data


def generate_access_token(name, lifespan):
    client = get_databand_context().databand_api_client
    resp = client.api_request(
        "/api/v1/auth/personal_access_token",
        {"label": name, "lifespan": lifespan},
        method="POST",
    )
    return resp


def create_airflow_instance(
    url,
    external_url,
    fetcher,
    env,
    dag_ids,
    last_seen_dag_run_id,
    last_seen_log_id,
    name,
    generate_token,
    system_alert_definitions,
    monitor_config,
):
    client = get_databand_context().databand_api_client
    endpoint = "airflow_monitor/add"
    request_data = {
        "base_url": url,
        "external_url": external_url,
        "fetcher": fetcher,
        "env": env,
        "monitor_config": monitor_config,
        "dag_ids": dag_ids,
        "last_seen_dag_run_id": last_seen_dag_run_id,
        "last_seen_log_id": last_seen_log_id,
        "name": name,
        "system_alert_definitions": system_alert_definitions,
    }

    if monitor_config:
        request_data["monitor_config"] = monitor_config

    resp = client.api_request(endpoint, request_data, method="POST")
    config_json = resp["server_info_dict"]
    config_json["core"][
        "databand_url"
    ] = get_databand_context().settings.core.databand_url

    if generate_token:
        token_resp = generate_access_token(name, generate_token)

        config_json["core"]["databand_access_token"] = token_resp["token"]

    return config_json


def edit_airflow_instance(
    tracking_source_uid,
    url,
    external_url,
    fetcher,
    env,
    dag_ids,
    last_seen_dag_run_id,
    last_seen_log_id,
    name,
    system_alert_definitions,
    monitor_config,
):
    client = get_databand_context().databand_api_client
    endpoint = "airflow_monitor/edit"
    request_data = {
        "tracking_source_uid": tracking_source_uid,
        "base_url": url,
        "external_url": external_url,
        "fetcher": fetcher,
        "env": env,
        "monitor_config": monitor_config,
        "dag_ids": dag_ids,
        "last_seen_dag_run_id": last_seen_dag_run_id,
        "last_seen_log_id": last_seen_log_id,
        "name": name,
        "system_alert_definitions": system_alert_definitions,
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
