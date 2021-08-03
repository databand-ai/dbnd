from __future__ import print_function

import logging

from dbnd._core.cli.utils import with_fast_dbnd_context
from dbnd._core.constants import AlertSeverity, RunState, TaskRunState
from dbnd._core.errors.base import DatabandApiError
from dbnd._core.utils.basics.text_banner import TextBanner, safe_tabulate
from dbnd._core.utils.cli import options_dependency, required_mutually_exclusive_options
from dbnd._vendor import click
from dbnd.api.alerts import (
    create_alert,
    delete_alerts,
    get_alerts_filtered,
    list_job_alerts,
)


logger = logging.getLogger(__name__)


class Severity(click.Choice):
    def __init__(self):
        super(Severity, self).__init__(
            choices=AlertSeverity.values(), case_sensitive=False
        )

    def convert(self, value, param, ctx):
        return super(Severity, self).convert(value.upper(), param, ctx)


class RunStateChoice(click.Choice):
    def __init__(self):
        super(RunStateChoice, self).__init__(
            choices=RunState.all_values(), case_sensitive=False
        )

    def convert(self, value, param, ctx):
        return super(RunStateChoice, self).convert(value.upper(), param, ctx)


class TaskRunStateChoice(click.Choice):
    def __init__(self):
        super(TaskRunStateChoice, self).__init__(
            choices=TaskRunState.all_values(), case_sensitive=False,
        )

    def convert(self, value, param, ctx):
        return super(TaskRunStateChoice, self).convert(value.upper(), param, ctx)


supported_alerts = {
    "maximum_duration": dict(
        value_help="Maximum duration of job run in seconds",
        value_type=click.INT,
        with_task=False,
        alert="RunDurationAlert",
        operator=">",
        metric="Run Duration (seconds)",
    ),
    "maximum_retries": dict(
        value_help="Maximum retries of job",
        value_type=click.INT,
        with_task=False,
        alert="MaxRunRetries",
        operator=">",
        metric="Number of Retries Per Run",
    ),
    "ran_last_x_seconds": dict(
        value_help="Duration in seconds of missing success run from job",
        value_type=click.INT,
        with_task=False,
        alert="RanLastXSecondsAlert",
        operator=">",
        metric="Delay Between Subsequent Runs (seconds)",
    ),
    "run_state": dict(
        value_help="State of the run to alert on",
        value_type=RunStateChoice(),
        with_task=False,
        alert="RunStateAlert",
        operator="==",
        metric="Run State",
    ),
    "task_state": dict(
        value_help="State of the task run to alert on",
        value_type=TaskRunStateChoice(),
        with_task=True,
        alert="TaskStateAlert",
        operator="==",
        metric="Task State",
    ),
    "custom_metric": dict(
        value_help="maximum threshold to match the metric",
        value_type=click.STRING,
        with_task=True,
        alert="CustomMetricAlert",
        operator=None,
        metric=None,
    ),
}


def bind_function(name, alert, with_task, value_help, value_type, operator, metric):
    def get_task_option(ctx):
        parent_params = ctx.parent.command.params
        return [op for op in parent_params if op.name == "task"][0]

    @with_fast_dbnd_context
    @click.option(
        "--value", "-v", help=value_help, type=value_type, required=True,
    )
    @click.pass_context
    def inner_function(ctx, value, metric_name=metric, op=operator):
        # xor is less readable
        if not with_task and ctx.obj.get("task"):
            raise click.BadParameter(
                "Task is set but it is unavailable for this alert",
                param=get_task_option(ctx),
                ctx=ctx,
            )
        if with_task and not ctx.obj.get("task"):
            raise click.MissingParameter(
                ctx=ctx,
                param=get_task_option(ctx),
                message="Task name is required for this alert",
            )

        cmd_create_alert(
            manage_ctx=ctx,
            alert=alert,
            operator=op,
            value=value,
            user_metric=metric_name,
        )

    if metric is None:
        inner_function = click.option(
            "--metric-name",
            "-m",
            help="Costume metric name to alert",
            type=click.STRING,
            required=True,
        )(inner_function)

    if operator is None:
        inner_function = click.option(
            "--op",
            "-o",
            help="your operator",
            type=click.Choice(["==", "!=", ">", ">=", "<", "<="]),
            required=True,
        )(inner_function)

    inner_function.__name__ = name

    return inner_function


@click.group()
@click.pass_context
def alerts(ctx):
    """Manage Databand alerts"""
    pass


CREATE_HELP_MSG = """
Create alert for given job\n
\n
EXAMPLES\n
    dbnd alerts create --job my_job --severity HIGH maximum-duration -v 100\n
    dbnd alerts create --job my_job --severity LOW maximum-retries -v 6\n
    dbnd alerts create --job my_job --severity CRITICAL ran-last-x-seconds -v 600\n
    dbnd alerts create --job my_job --severity MEDIUM run-state -v failed\n
    dbnd alerts create --job my_job --severity MEDIUM --task my_job_task task-state -v cancelled\n
    dbnd alerts create --job my_job --severity CRITICAL --task my_job_task custom-metric --metric-name my_metric -v 100 --op "<="\n
"""


@alerts.group(help=CREATE_HELP_MSG)
@click.option("--severity", "-s", help="Alert severity", type=Severity(), required=True)
@click.option("--job", "-j", help="Job name", type=click.STRING, required=False)
@click.option("--job-id", type=click.STRING, required=False)
@click.option("--task", "-t", help="Task name", type=click.STRING, required=False)
@click.option(
    "--update",
    "-u",
    "uid",
    help="Uid of an existing alert to update",
    type=click.STRING,
    required=False,
)
@click.pass_context
def create(ctx, job, job_id, severity, task, uid):
    """Create or update alerts for given job."""
    ctx.obj = {
        "job": job,
        "job_id": job_id,
        "severity": severity,
        "task": task,
        "uid": uid,
    }


# Registering the create commands
for name, values in supported_alerts.items():
    f = bind_function(name, **values)
    create.command(name=name.replace("_", "-"))(f)


@alerts.command("list")
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
@with_fast_dbnd_context
def list_jobs(job):
    """Display alert definitions for given job"""
    try:
        alerts_list = list_job_alerts(job_name=job)
    except LookupError as e:
        logger.warning(e)
    else:
        print_table("Alerts configured for {job}".format(job=job), alerts_list)


def print_table(header, alerts_list):
    banner = TextBanner(header)
    banner.write(build_alerts_table(alerts_list))
    logger.info(banner.get_banner_str())


def build_alerts_table(alerts_data):
    extract_keys = (
        "uid",
        "job_name",
        "task_name",
        "custom_name",
        "severity",
        "type",
        "operator",
        "value",
        "user_metric",
    )
    headers = (
        "uid",
        "job",
        "task",
        "name",
        "severity",
        "type",
        "op",
        "value",
        "metric",
    )
    table_data = []
    for alert in alerts_data:
        alert_row = [alert.get(key, "") for key in extract_keys]
        table_data.append(alert_row)
    return safe_tabulate(table_data, headers)


def cmd_create_alert(manage_ctx, alert, operator, value, user_metric):
    try:
        alert_def_uid = create_alert(
            job_name=manage_ctx.obj.get("job"),
            job_id=manage_ctx.obj.get("job_id"),
            task_name=manage_ctx.obj.get("task", None),
            uid=manage_ctx.obj.get("uid", None),
            alert_class=alert,
            severity=manage_ctx.obj["severity"],
            operator=operator,
            value=value,
            user_metric=user_metric,
        )
    except DatabandApiError as e:
        logger.warning("failed with - {}".format(e.response))
    else:
        logger.info("Created alert %s", alert_def_uid["uid"])


@alerts.command()
@with_fast_dbnd_context
@options_dependency("job", "name")
@required_mutually_exclusive_options("uid", "wipe", "job")
@click.option("--uid", "-u", help="alert uid", type=click.STRING)
@click.option("--wipe", help="delete all alerts", is_flag=True)
@click.option("--job", "-j", help="job name", type=click.STRING)
@click.option("--name", "-n", help="alert custom name", type=click.STRING)
def delete(uid, wipe, job, name):
    """Delete alerts"""
    if wipe:
        alerts_list = get_alerts_filtered()
    elif uid:
        alerts_list = get_alerts_filtered(alert_uid=uid)
    else:
        alerts_list = get_alerts_filtered(job_name=job, custom_name=name)
    if alerts_list:
        uids = [alert["uid"] for alert in alerts_list]
        delete_alerts(uids)
        print_table("deleted", alerts_list)
    else:
        logger.warning("alerts not found to delete")
