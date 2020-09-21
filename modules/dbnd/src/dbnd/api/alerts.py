import logging

from dbnd._core.current import get_databand_context
from dbnd.api.query_params import build_query_api_params, create_filters_builder
from dbnd.api.scheduler import get_scheduled_job_by_name
from dbnd.api.shared_schemas.alerts_def_schema import AlertDefsSchema


logger = logging.getLogger(__name__)


def _build_alert(alert_class, scheduled_job, severity, operator, value, label):
    """build alert for scheduled job"""

    return {
        "type": alert_class,
        "scheduled_job_uid": scheduled_job["uid"],
        "operator": operator,
        "value": str(value),
        "severity": severity,
        "user_metric": label,
    }


def _post_alert(client, alert):
    alert_uid = client.api_request(endpoint="alert_defs", data=alert, method="POST")
    return alert_uid


def upsert_alert(alert_class, scheduled_job_name, severity, operator, value, label):
    """add/edit alert for existing scheduled job"""
    scheduled_job = get_scheduled_job_by_name(scheduled_job_name)
    if scheduled_job:
        alert = _build_alert(
            alert_class, scheduled_job, severity, operator, value, label
        )
        alert_def_uid = _post_alert(get_databand_context().databand_api_client, alert)
        logger.info("Created/updated alert %s", alert_def_uid["uid"])
    else:
        logger.error("scheduled job with name '%s' not found" % scheduled_job_name)


def _get_alerts(client, query_api_args):
    endpoint = "?".join(["alert_defs", query_api_args])
    schema = AlertDefsSchema(strict=False)
    response = client.api_request(endpoint=endpoint, data="", method="GET")
    return schema.load(data=response["data"], many=True).data


build_alerts_filter = create_filters_builder(
    job_name=("job_name", "eq"),
    severity=("severity", "eq"),
    alert_type=("type", "eq"),
    task_name=("task_name", "eq"),
    scheduled_job_uid=("scheduled_job_uid", "eq"),
)


def get_alerts_filtered(**kwargs):
    """get alerts by filters"""
    query_params = build_query_api_params(filters=build_alerts_filter(**kwargs))
    return _get_alerts(get_databand_context().databand_api_client, query_params)


def delete_alerts(uids):
    """delete alerts by uids"""
    get_databand_context().databand_api_client.api_request(
        endpoint="alert_defs/delete", data=uids, method="POST"
    )


if __name__ == "__main__":
    print(get_alerts_filtered(scheduled_job_uid="41f2b236-f696-11ea-b2cb-acde48001122"))
