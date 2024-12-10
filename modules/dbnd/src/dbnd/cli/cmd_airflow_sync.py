# © Copyright Databand.ai, an IBM Company 2022

import json
import logging

from dbnd._core.errors.base import DatabandApiError, DatabandBadRequest
from dbnd._core.utils.basics.text_banner import TextBanner, safe_tabulate
from dbnd._vendor import click
from dbnd._vendor.marshmallow import ValidationError, validate
from dbnd.api.airflow_sync import (
    archive_airflow_instance,
    create_airflow_instance,
    edit_airflow_instance,
    list_synced_airflow_instances,
)


BYTES_IN_KB = 1024
logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def airflow_sync(ctx):
    """Manage synced Airflow instances"""
    # we need to load configs,
    # we have multiple get_databand_context().databand_api_client calls

    from dbnd import dbnd_config

    dbnd_config.load_system_configs()


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
        "name",
        "is_sync_enabled",
        "tracking_source_uid",
        "base_url",
        "external_url",
        "api_mode",
    )
    headers = (
        "Name",
        "Active",
        "Tracking source UID",
        "Base url",
        "External url",
        "Api Mode",
        "Instance Type",
    )
    table_data = []
    for server_info in instances_data:
        instance_row = [getattr(server_info, key, "") for key in extract_keys]
        instance_row.append("Airflow")
        table_data.append(instance_row)
    return safe_tabulate(table_data, headers)


@airflow_sync.command()
@click.option(
    "--name", "-n", help="Name for the syncer", type=click.STRING, required=True
)
@click.option(
    "--url", "-u", help="base url for instance", type=click.STRING, required=True
)
@click.option(
    "--external-url",
    "-e",
    help="External url for instance",
    type=click.STRING,
    default=None,
)
@click.option(
    "--fetcher",
    "-f",
    help="Fetcher to use for data (web, db)",
    type=click.Choice(["web", "db"], case_sensitive=False),
    default="db",
)
@click.option("--env", help="Environment", type=click.STRING, default=None)
@click.option(
    "--include-sources",
    help="Monitor source code for tasks",
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
    "--generate-token",
    help="Generate access token for the syncer, value is token lifespan (in seconds)",
    type=click.INT,
    default=None,
)
@click.option(
    "--config-file-output",
    help="Store syncer config json to file",
    type=click.File("w"),
    default="-",
)
@click.option(
    "--with-auto-alerts",
    help="Create syncer with auto alerts config",
    type=click.BOOL,
    is_flag=True,
)
@click.option(
    "--include-logs-bytes-from-head",
    help="Include the number of bytes from the head of the log file",
    type=click.IntRange(min=0, max=8096),
    default=0,
)
@click.option(
    "--include-logs-bytes-from-end",
    help="Include the number of bytes from the end of the log file",
    type=click.IntRange(min=0, max=8096),
    default=0,
)
@click.option(
    "--dag-run-bulk-size",
    help="DAG run bulk size for the syncer",
    type=click.INT,
    default=None,
)
def add(
    name,
    url,
    external_url,
    fetcher,
    env,
    include_sources,
    dag_ids,
    last_seen_dag_run_id,
    last_seen_log_id,
    generate_token,
    config_file_output,
    with_auto_alerts,
    include_logs_bytes_from_head,
    include_logs_bytes_from_end,
    dag_run_bulk_size,
):
    try:
        if not env:
            env = name

        system_alert_definitions = {
            "failed_state": bool(with_auto_alerts),
            "ml_run_duration": bool(with_auto_alerts),
            "run_schema_change": bool(with_auto_alerts),
        }

        monitor_config = {
            "include_sources": bool(include_sources),
            "log_bytes_from_end": include_logs_bytes_from_end * BYTES_IN_KB,
            "log_bytes_from_head": include_logs_bytes_from_head * BYTES_IN_KB,
        }

        if dag_run_bulk_size:
            monitor_config["dag_run_bulk_size"] = dag_run_bulk_size

        validate.URL(schemes={"http", "https"}, require_tld=False)(url)
        config_json = create_airflow_instance(
            url,
            external_url,
            fetcher,
            env,
            dag_ids,
            last_seen_dag_run_id,
            last_seen_log_id,
            name,
            generate_token,
            system_alert_definitions,
            monitor_config,
        )

        if config_file_output:
            json.dump(config_json, config_file_output, indent=4)
    except ValidationError:
        raise DatabandBadRequest(
            f"Could not edit airflow syncer because url {url} was not a valid url"
        )
    except DatabandApiError as e:
        logger.warning(f"failed with - {e.response}")
    else:
        logger.info("Successfully added %s", url)


@airflow_sync.command()
@click.option(
    "--tracking-source-uid",
    "-t",
    help='Tracking source uid of the edited airflow syncer. (you can get this with the "list" command)',
    type=click.STRING,
    required=True,
)
@click.option(
    "--url", "-u", help="base url for instance", type=click.STRING, required=True
)
@click.option(
    "--external-url",
    "-e",
    help="External url for instance",
    type=click.STRING,
    default=None,
)
@click.option(
    "--fetcher",
    "-f",
    help="Fetcher to use for data (web, db)",
    type=click.Choice(["web", "db"], case_sensitive=False),
    default="db",
)
@click.option("--env", help="Environment", type=click.STRING, default=None)
@click.option(
    "--include-sources",
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
@click.option("--name", help="Name for the syncer", type=click.STRING, default=None)
@click.option(
    "--with-auto-alerts",
    help="Create syncer with auto alerts config",
    type=click.BOOL,
    is_flag=True,
)
@click.option(
    "--include-logs-bytes-from-head",
    help="Include the number of bytes from the head of the log file",
    type=click.IntRange(min=0, max=8096),
    default=0,
)
@click.option(
    "--include-logs-bytes-from-end",
    help="Include the number of bytes from the end of the log file",
    type=click.IntRange(min=0, max=8096),
    default=0,
)
@click.option(
    "--dag-run-bulk-size",
    help="DAG run bulk size for the syncer",
    type=click.INT,
    default=None,
)
def edit(
    tracking_source_uid,
    url,
    external_url,
    fetcher,
    env,
    include_sources,
    dag_ids,
    last_seen_dag_run_id,
    last_seen_log_id,
    name,
    with_auto_alerts,
    include_logs_bytes_from_head,
    include_logs_bytes_from_end,
    dag_run_bulk_size,
):
    try:
        system_alert_definitions = {
            "failed_state": bool(with_auto_alerts),
            "ml_run_duration": bool(with_auto_alerts),
            "run_schema_change": bool(with_auto_alerts),
        }

        monitor_config = {
            "include_sources": bool(include_sources),
            "log_bytes_from_end": include_logs_bytes_from_end * BYTES_IN_KB,
            "log_bytes_from_head": include_logs_bytes_from_head * BYTES_IN_KB,
        }

        if dag_run_bulk_size:
            monitor_config["dag_run_bulk_size"] = dag_run_bulk_size

        validate.URL(schemes={"http", "https"}, require_tld=False)(url)
        edit_airflow_instance(
            tracking_source_uid,
            url,
            external_url,
            fetcher,
            env,
            dag_ids,
            last_seen_dag_run_id,
            last_seen_log_id,
            name,
            system_alert_definitions,
            monitor_config,
        )
    except ValidationError:
        raise DatabandBadRequest(
            f"Could not edit airflow syncer because url {url} was not a valid url"
        )
    except DatabandApiError as e:
        logger.warning("failed with - {}".format(e.response))
    else:
        logger.info("Successfully edited %s", url)


@airflow_sync.command()
@click.option(
    "--tracking-source-uid",
    "-t",
    help='Tracking source uid of the archived airflow syncer. (you can get this with the "list" command)',
    type=click.STRING,
    required=True,
)
def archive(tracking_source_uid):
    try:
        archive_airflow_instance(tracking_source_uid)
    except DatabandApiError as e:
        logger.warning("failed with - {}".format(e.response))
    else:
        logger.info("Archived instance %s", tracking_source_uid)
