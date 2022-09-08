# © Copyright Databand.ai, an IBM Company 2022

import json
import logging

from dbnd._core.constants import AlertDefOperator
from dbnd._core.current import get_databand_context
from dbnd.api.jobs import is_job_exists
from dbnd.api.scheduler import is_scheduled_job_exists
from dbnd.api.shared_schemas.alerts_def_schema import AlertDefsSchema


logger = logging.getLogger(__name__)


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
    job_name,
    job_id,
    task_name,
    task_repr,
    uid,
    alert_class,
    severity,
    operator,
    value,
    user_metric,
    is_str_value=False,
    **optional_fields,
):
    alert = {
        "type": alert_class,
        "job_name": job_name,
        "job_id": job_id,
        "task_name": task_name,
        "task_repr": task_repr,
        "uid": uid,
        "operator": operator,
        "value": value,
        "severity": severity,
        "user_metric": user_metric,
        "is_str_value": is_str_value,
    }
    if operator == AlertDefOperator.ANOMALY.value:
        alert["ml_alert"] = {
            "look_back": optional_fields["look_back"],
            "sensitivity": optional_fields["sensitivity"],
        }
    if operator == AlertDefOperator.RANGE.value:
        alert["operator"] = "RangeAlert"
        alert["value"] = json.dumps(
            {"baseline": optional_fields["baseline"], "range": optional_fields["range"]}
        )
    alert = {key: value for key, value in alert.items() if value is not None}
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


def get_alerts_filtered(alert_def_uid=None, job_name=None, custom_name=None):
    """get alerts by filters"""
    params = {"uids": alert_def_uid, "job_name": job_name, "custom_name": custom_name}
    query_params = "&".join(
        f"{key}={value}" for key, value in params.items() if value is not None
    )
    return _get_alerts(get_databand_context().databand_api_client, query_params)


def delete_alerts(uids):
    """delete alerts by uids"""
    get_databand_context().databand_api_client.api_request(
        endpoint="alert_defs/delete", data=uids, method="POST"
    )
