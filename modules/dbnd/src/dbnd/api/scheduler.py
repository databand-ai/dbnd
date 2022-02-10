from collections import namedtuple

from more_itertools import first

from dbnd._core.current import get_databand_context
from dbnd.api.query_params import build_query_api_params, create_filters_builder
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
    name_pattern=None, from_file_only=None, include_deleted=False, ctx=None
):
    ctx = ctx or get_databand_context()
    query_params = build_query_api_params(
        filters=build_scheduled_job_filter(
            name_pattern=name_pattern,
            from_file_flag=from_file_only,
            include_deleted_flag=include_deleted,
        )
    )
    return _get_scheduled_jobs(ctx.databand_api_client, query_params)


def set_scheduled_job_active(name, value, ctx=None):
    ctx = ctx or get_databand_context()
    ctx.databand_api_client.api_request(
        "/api/v1/scheduled_jobs/set_active?name=%s&value=%s"
        % (name, str(value).lower()),
        None,
        method="PUT",
        no_prefix=True,
    )


def _get_scheduled_jobs(client, query_api_args):
    endpoint = "?".join(["scheduled_jobs", query_api_args])
    schema = ScheduledJobSchemaV2(strict=False)
    response = client.api_request(endpoint=endpoint, data=None, method="GET")
    return schema.load(data=response["data"], many=True).data


def get_scheduled_job_by_name(job_name):
    query_params = build_query_api_params(
        filters=build_scheduled_job_filter(job_name=job_name)
    )

    job_result = _get_scheduled_jobs(
        get_databand_context().databand_api_client, query_params
    )
    try:
        return first(job_result)["DbndScheduledJob"]
    except ValueError:
        return None


def is_scheduled_job_exists(job_name):
    return bool(get_scheduled_job_by_name(job_name))


build_scheduled_job_filter = create_filters_builder(
    job_name=("name", "eq"),
    name_pattern=("name", "like"),
    from_file_flag=("from_file", "eq"),
    include_deleted_flag=("deleted_from_file", "eq"),
)
