import logging

from dbnd._core.errors.base import DatabandApiError
from dbnd._core.utils.basics.text_banner import TextBanner, safe_tabulate
from dbnd._vendor import click
from dbnd.api.airflow_sync import (
    archive_airflow_instance,
    create_airflow_instance,
    list_synced_airflow_instances,
    unarchive_airflow_instance,
)


logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def airflow_sync(ctx):
    """Manage synced Airflow instances"""
    pass


@airflow_sync.command("list")
def list_airflow_instances():
    try:
        instances = list_synced_airflow_instances()
    except (LookupError, DatabandApiError) as e:
        logger.warning(e)
    else:
        print_table("Synced Airflow instances", instances)


def print_table(header, instances_list):
    banner = TextBanner(header)
    banner.write(build_instances_table(instances_list))
    logger.info(banner.get_banner_str())


def build_instances_table(instances_data):
    extract_keys = (
        "is_sync_enabled",
        "base_url",
        "external_url",
        "api_mode",
    )
    headers = (
        "Active",
        "Base url",
        "External url",
        "Api Mode",
        "Instance Type",
    )
    table_data = []
    for server_info in instances_data:
        instance_row = [getattr(server_info, key, "") for key in extract_keys]
        instance_row.append(
            "Airflow" if getattr(server_info, "fetcher") == "web" else "GC Composer"
        )
        table_data.append(instance_row)
    return safe_tabulate(table_data, headers)


def validate_composer_id(ctx, param, value):
    if ctx.params["instance_type"] == "composer":
        if value is None:
            raise click.MissingParameter(
                "Client id is required for Google Cloud Composer instance", ctx, param
            )
    else:
        if value is not None:
            raise click.BadParameter(
                "Setting client id is allowed only for Google Cloud Composer instances",
                ctx,
                param,
            )
    return value


@airflow_sync.command()
@click.option(
    "--url", "-u", help="base url for instance", type=click.STRING, required=True
)
@click.option(
    "--external-url", "-e", help="external url for instance", type=click.STRING
)
@click.option(
    "--instance-type",
    "-i",
    help="type of an instance: Airflow or Google Cloud Composer",
    type=click.Choice(["airflow", "composer"], case_sensitive=False),
    required=True,
)
@click.option(
    "--api-mode",
    "-a",
    help="Airflow API connection mode",
    type=click.Choice(["rbac", "flask-admin", "experimental"]),
    default="rbac",
)
@click.option(
    "--composer-client-id",
    "-c",
    help="client id for Google Cloud Composer connection",
    type=click.STRING,
    callback=validate_composer_id,
)
@click.option(
    "--fetch-quantity",
    help="How much data to fetch in each iteration",
    type=click.INT,
    default=100,
)
@click.option(
    "--oldest-incomplete-data-in-days",
    help="Number of days ago to look for incomplete data",
    type=click.INT,
    default=14,
)
@click.option(
    "--include-logs",
    help="Should monitor include logs for tasks",
    type=click.BOOL,
    is_flag=True,
)
@click.option(
    "--include-task-args",
    help="Should monitor include task args for tasks",
    type=click.BOOL,
    is_flag=True,
)
@click.option(
    "--include-xcom",
    help="Should monitor include xcom data for tasks",
    type=click.BOOL,
    is_flag=True,
)
@click.option(
    "--exclude-sources",
    help="Don't monitor source code for tasks",
    type=click.BOOL,
    is_flag=True,
)
@click.option(
    "--dag-ids",
    help="List of specific dag ids (separated with comma) that monitor will fetch only from them",
    type=click.STRING,
    default=None,
)
@click.option(
    "--last-seen-dag-run-id",
    help="Id of the last dag run seen in the Airflow database",
    type=click.STRING,
    default=None,
)
@click.option(
    "--last-seen-log-id",
    help="Id of the last log seen in the Airflow database",
    type=click.STRING,
    default=None,
)
@click.option(
    "--use-af-monitor-v2",
    help="Use Airflow monitor v2 for this source",
    type=click.BOOL,
    is_flag=True,
)
def add(
    url,
    external_url,
    instance_type,
    api_mode,
    composer_client_id,
    fetch_quantity,
    oldest_incomplete_data_in_days,
    include_logs,
    include_task_args,
    include_xcom,
    exclude_sources,
    dag_ids,
    last_seen_dag_run_id,
    last_seen_log_id,
    use_af_monitor_v2,
):
    fetcher = "web" if instance_type == "airflow" else instance_type
    try:
        create_airflow_instance(
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
            not exclude_sources,
            dag_ids,
            last_seen_dag_run_id,
            last_seen_log_id,
            use_af_monitor_v2,
        )
    except DatabandApiError as e:
        logger.warning("failed with - {}".format(e.response))
    else:
        logger.info("Starting syncing instance %s", url)


@airflow_sync.command()
@click.option(
    "--url",
    "-u",
    help="base url of an instance to archive",
    type=click.STRING,
    required=True,
)
def archive(url):
    try:
        archive_airflow_instance(url)
    except DatabandApiError as e:
        logger.warning("failed with - {}".format(e.response))
    else:
        logger.info("Archived instance %s", url)


@airflow_sync.command()
@click.option(
    "--url",
    "-u",
    help="base url of an instance to unarchive",
    type=click.STRING,
    required=True,
)
def unarchive(url):
    try:
        unarchive_airflow_instance(url)
    except DatabandApiError as e:
        logger.warning("failed with - {}".format(e.response))
    else:
        logger.info("Unarchived instance %s", url)
