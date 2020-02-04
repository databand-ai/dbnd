import json

from collections import namedtuple
from typing import Any, Dict

from dbnd._core.configuration.dbnd_config import config
from dbnd.api.api_utils import ApiClient
from dbnd.api.shared_schemas.scheduled_job_schema import ScheduledJobSchemaV2


config.load_system_configs()
api_client = ApiClient(
    config.get("core", "databand_url"),
    auth=True,
    user=config.get("scheduler", "dbnd_user"),
    password=config.get("scheduler", "dbnd_password"),
)


schema = ScheduledJobSchemaV2(strict=False)

ScheduledJobNamedTuple = namedtuple("ScheduledJobNamedTuple", schema.fields.keys())
ScheduledJobNamedTuple.__new__.__defaults__ = (None,) * len(
    ScheduledJobNamedTuple._fields
)


def post_scheduled_job(scheduled_job_dict):
    data, _ = schema.dump({"DbndScheduledJob": scheduled_job_dict})
    response = api_client.api_request(
        "/api/v1/scheduled_jobs", data, method="POST", no_prefix=True
    )
    return schema.load(data=response).data


def patch_scheduled_job(scheduled_job_dict):
    data, _ = schema.dump({"DbndScheduledJob": scheduled_job_dict})
    response = api_client.api_request(
        "/api/v1/scheduled_jobs?name=%s" % scheduled_job_dict["name"],
        data,
        method="PATCH",
        no_prefix=True,
    )
    return schema.load(data=response).data


def delete_scheduled_job(scheduled_job_name, revert=False):
    api_client.api_request(
        "/api/v1/scheduled_jobs?name=%s&revert=%s"
        % (scheduled_job_name, str(revert).lower()),
        None,
        method="DELETE",
        no_prefix=True,
    )


def get_scheduled_jobs(name_pattern=None, from_file_only=False, include_deleted=False):
    query_filter = []
    if from_file_only:
        query_filter.append({"name": "from_file", "op": "eq", "val": True})

    if not include_deleted:
        query_filter.append({"name": "deleted_from_file", "op": "eq", "val": False})

    if name_pattern:
        query_filter.append({"name": "name", "op": "like", "val": name_pattern})

    query = {"filter": json.dumps(query_filter)}

    res = api_client.api_request(
        "/api/v1/scheduled_jobs", None, method="GET", query=query, no_prefix=True
    )
    return schema.load(data=res["data"], many=True).data


def set_active(name, value):
    api_client.api_request(
        "/api/v1/scheduled_jobs/set_active?name=%s&value=%s"
        % (name, str(value).lower()),
        None,
        method="PUT",
        no_prefix=True,
    )
