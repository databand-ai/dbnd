import base64
import logging
import os
import tarfile

from io import BytesIO

from dbnd._core.utils.cli import required_mutually_exclusive_options
from dbnd._vendor import click


logger = logging.getLogger(__name__)


@click.command()
def ipython():
    """Get ipython shell with Databand's context"""
    # noinspection PyUnresolvedReferences
    from dbnd_web import models  # noqa
    from dbnd import new_dbnd_context
    from airflow.utils.db import create_session
    import IPython

    with new_dbnd_context(
        name="ipython", autoload_modules=False
    ) as ctx, create_session() as session:
        header = "\n\t".join(
            [
                "Welcome to \033[91mDataband\033[0m's ipython command.\nPredefined variable are",
                "\033[92m\033[1mctx\033[0m     -> dbnd_context",
                "\033[92m\033[1msession\033[0m -> DB session",
                "\033[92m\033[1mmodels\033[0m  -> dbnd models",
            ]
        )
        IPython.embed(colors="neutral", header=header)


@click.command(
    help="Collect logs and debugging information for a specific DatabandRun. Creates a tarfile that can be "
    "easily sent to databand.ai customer support"
)
@required_mutually_exclusive_options("uid", "name")
@click.option("--name", "-n", help="The name of the databand run to retrieve logs for")
@click.option("--uid", "-u", help="The UUID of the databand run to retrieve logs for")
def collect_logs(name, uid):
    # Click performs parameter validation, mutually exclusive options

    output_filename, tar_file_data = send_collect_logs_api_request(name, uid)
    working_directory = os.getcwd()
    with open(os.path.join(working_directory, output_filename), "wb") as output_file:
        output_file.write(base64.b64decode(tar_file_data))
    logger.info(
        "Successfully written tarfile %s to disk! Please send it to our customer support to continue the "
        "debugging process!" % output_filename
    )


def send_collect_logs_api_request(name, uid):
    from dbnd import get_databand_context
    from dbnd._core.errors.base import DatabandApiError
    from dbnd._core.errors.friendly_error.api import couldnt_find_databand_run_in_db

    COLLECT_LOGS_ENDPOINT = "runs/collect_error_info"

    query = {}
    using_name = True
    if name:
        query["run_name"] = name
    elif uid:
        using_name = False
        query["run_uid"] = uid

    try:
        api_client = get_databand_context().databand_api_client
        response = api_client.api_request(
            endpoint=COLLECT_LOGS_ENDPOINT, method="GET", query=query, data={}
        )
        return response["name"], response["data"]
    except DatabandApiError as e:
        if using_name:
            raise couldnt_find_databand_run_in_db(query["run_name"], e)
        else:
            raise couldnt_find_databand_run_in_db(query["run_uid"], e)
