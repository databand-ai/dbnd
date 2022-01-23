import json
import logging

from dbnd._core.constants import AirflowEnvironment
from dbnd._core.errors.base import DatabandApiError
from dbnd._core.utils.basics.text_banner import TextBanner, safe_tabulate
from dbnd._vendor import click
from dbnd.api.airflow_sync import (
    archive_airflow_instance,
    create_airflow_instance,
    edit_airflow_instance,
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


@airflow_sync.command()
@click.option(
    "--url", "-u", help="base url for instance", type=click.STRING, required=True
)
@click.option(
    "--external-url", "-e", help="external url for instance", type=click.STRING
)
@click.option(
    "--fetcher",
    "-f",
    help="Fetcher to use ofr data (web, db)",
    type=click.Choice(["web", "db"], case_sensitive=False),
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
    "--airflow-environment",
    "-t",
    help="Airflow environment",
    type=click.Choice(AirflowEnvironment.all_values()),
    default="on_prem",
)
@click.option(
    "--composer-client-id",
    "-c",
    help="client id for Google Cloud Composer connection",
    type=click.STRING,
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
    "--name", help="Name for the syncer", type=click.STRING, default=None,
)
@click.option(
    "--generate-token",
    help="Generate access token for the syncer, value is token lifespan",
    type=click.INT,
    default=None,
)
@click.option(
    "--output",
    help="Store syncer config json to file",
    type=click.File("w"),
    default="-",
)
@click.option(
    "--with-auto-alerts",
    help="Create syncer with auto alert config",
    type=click.BOOL,
    default=True,
)
def add(
    url,
    external_url,
    fetcher,
    api_mode,
    airflow_environment,
    composer_client_id,
    exclude_sources,
    dag_ids,
    last_seen_dag_run_id,
    last_seen_log_id,
    name,
    generate_token,
    output,
    with_auto_alerts,
):
    try:

        if with_auto_alerts:
            system_alert_definitions = {
                "failed_state": True,
                "ml_run_duration": True,
                "run_schema_change": True,
            }
        else:
            system_alert_definitions = {}

        config_json = create_airflow_instance(
            url,
            external_url,
            fetcher,
            api_mode,
            airflow_environment,
            composer_client_id,
            not exclude_sources,
            dag_ids,
            last_seen_dag_run_id,
            last_seen_log_id,
            name,
            generate_token,
            system_alert_definitions,
        )
        if output:
            json.dump(config_json, output, indent=4)
    except DatabandApiError as e:
        logger.warning("failed with - {}".format(e.response))
    else:
        logger.info("Successfully added %s", url)


@airflow_sync.command()
@click.option(
    "--url", "-u", help="base url for instance", type=click.STRING, required=True
)
@click.option(
    "--external-url", "-e", help="external url for instance", type=click.STRING
)
@click.option(
    "--fetcher",
    "-f",
    help="Fetcher to use ofr data (web, db)",
    type=click.Choice(["web", "db"], case_sensitive=False),
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
    "--airflow-environment",
    "-t",
    help="Airflow environment",
    type=click.Choice(AirflowEnvironment.all_values()),
    default="on_prem",
)
@click.option(
    "--composer-client-id",
    "-c",
    help="client id for Google Cloud Composer connection",
    type=click.STRING,
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
    "--name", help="Name for the syncer", type=click.STRING, default=None,
)
def edit(
    url,
    external_url,
    fetcher,
    api_mode,
    airflow_environment,
    composer_client_id,
    exclude_sources,
    dag_ids,
    last_seen_dag_run_id,
    last_seen_log_id,
    name,
):
    try:
        edit_airflow_instance(
            url,
            external_url,
            fetcher,
            api_mode,
            airflow_environment,
            composer_client_id,
            not exclude_sources,
            dag_ids,
            last_seen_dag_run_id,
            last_seen_log_id,
            name,
        )
    except DatabandApiError as e:
        logger.warning("failed with - {}".format(e.response))
    else:
        logger.info("Successfully edited %s", url)


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
