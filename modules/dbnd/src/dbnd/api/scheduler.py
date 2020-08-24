import json

from collections import namedtuple
from typing import Any, Dict

from dbnd._core.current import get_databand_context
from dbnd.api.shared_schemas.scheduled_job_schema import ScheduledJobSchemaV2


ScheduledJobNamedTuple = namedtuple(
    "ScheduledJobNamedTuple", ScheduledJobSchemaV2().fields.keys()
)
ScheduledJobNamedTuple.__new__.__defaults__ = (None,) * len(
    ScheduledJobNamedTuple._fields
)


def post_scheduled_job(scheduled_job_dict, ctx=None):
    ctx = ctx or get_databand_context()
    schema = ScheduledJobSchemaV2(strict=False)
    data, _ = schema.dump({"DbndScheduledJob": scheduled_job_dict})
    response = ctx.databand_api_client.api_request(
        "/api/v1/scheduled_jobs", data, method="POST", no_prefix=True
    )
    return schema.load(data=response).data


def patch_scheduled_job(scheduled_job_dict, ctx=None):
    ctx = ctx or get_databand_context()
    schema = ScheduledJobSchemaV2(strict=False)
    data, _ = schema.dump({"DbndScheduledJob": scheduled_job_dict})
    response = ctx.databand_api_client.api_request(
        "/api/v1/scheduled_jobs?name=%s" % scheduled_job_dict["name"],
        data,
        method="PATCH",
        no_prefix=True,
    )
    return schema.load(data=response).data


def delete_scheduled_job(scheduled_job_name, revert=False, ctx=None):
    ctx = ctx or get_databand_context()
    ctx.databand_api_client.api_request(
        "/api/v1/scheduled_jobs?name=%s&revert=%s"
        % (scheduled_job_name, str(revert).lower()),
        None,
        method="DELETE",
        no_prefix=True,
    )


def get_scheduled_jobs(
    name_pattern=None, from_file_only=False, include_deleted=False, ctx=None
):
    ctx = ctx or get_databand_context()
    schema = ScheduledJobSchemaV2(strict=False)
    query_filter = []
    if from_file_only:
        query_filter.append({"name": "from_file", "op": "eq", "val": True})

    if not include_deleted:
        query_filter.append({"name": "deleted_from_file", "op": "eq", "val": False})

    if name_pattern:
        query_filter.append({"name": "name", "op": "like", "val": name_pattern})

    query = {"filter": json.dumps(query_filter)}

    res = ctx.databand_api_client.api_request(
        "/api/v1/scheduled_jobs", None, method="GET", query=query, no_prefix=True
    )
    return schema.load(data=res["data"], many=True).data


def set_scheduled_job_active(name, value, ctx=None):
    ctx = ctx or get_databand_context()
    ctx.databand_api_client.api_request(
        "/api/v1/scheduled_jobs/set_active?name=%s&value=%s"
        % (name, str(value).lower()),
        None,
        method="PUT",
        no_prefix=True,
    )
