import logging

from dbnd._core.current import get_databand_context
from dbnd.api.jobs import is_job_exists
from dbnd.api.query_params import build_query_api_params, create_filters_builder
from dbnd.api.scheduler import is_scheduled_job_exists
from dbnd.api.shared_schemas.alerts_def_schema import AlertDefsSchema


logger = logging.getLogger(__name__)


def not_none_dict(d):
    return {key: value for key, value in d.items() if value is not None}


def _build_alert(
    job_name, task_name, uid, alert_class, severity, operator, value, user_metric
):
    """build alert for job"""

    return not_none_dict(
        {
            "type": alert_class,
            "job_name": job_name,
            "task_name": task_name,
            "uid": uid,
            "operator": operator,
            "value": str(value),
            "severity": severity,
            "user_metric": user_metric,
        }
    )


def run_if_job_exists(func):
    def inner(job_name, *args, **kwargs):
        if is_job_exists(job_name) or is_scheduled_job_exists(job_name):
            return func(job_name, *args, **kwargs)
        else:
            raise LookupError("Job named '%s' is not found" % job_name)

    return inner


def _post_alert(client, alert):
    alert_uid = client.api_request(endpoint="alert_defs", data=alert, method="POST")
    return alert_uid


def create_alert(
    job_name, task_name, uid, alert_class, severity, operator, value, user_metric
):
    """add alert for existing job or scheduled job"""
    alert = _build_alert(
        job_name, task_name, uid, alert_class, severity, operator, value, user_metric
    )
    alert_def_uid = _post_alert(get_databand_context().databand_api_client, alert)
    return alert_def_uid


@run_if_job_exists
def list_job_alerts(job_name):
    return get_alerts_filtered(job_name=job_name)


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
    alert_uid=("uid", "eq"),
    custom_name=("custom_name", "eq"),
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
