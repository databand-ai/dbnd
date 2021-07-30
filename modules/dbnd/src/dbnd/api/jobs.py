from more_itertools import first

from dbnd._core.current import get_databand_context
from dbnd.api.query_params import build_query_api_params, create_filter_builder
from dbnd.api.shared_schemas.job_schema import JobSchemaV2


def get_job_by_name(job_name):
    query_params = build_query_api_params(filters=build_job_name_filter(job_name))

    job_result = _get_job(get_databand_context().databand_api_client, query_params)
    return first(job_result, None)


def is_job_exists(job_name):
    return bool(get_job_by_name(job_name))


def _get_job(client, query_api_args):
    endpoint = "?".join(["jobs", query_api_args])
    schema = JobSchemaV2(strict=False)
    response = client.api_request(endpoint=endpoint, data="", method="GET",)
    return schema.load(data=response["data"], many=True).data


build_job_name_filter = create_filter_builder("name", "eq")
