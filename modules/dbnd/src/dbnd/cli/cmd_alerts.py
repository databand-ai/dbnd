from __future__ import print_function

import logging

from dbnd._core.cli.utils import with_fast_dbnd_context
from dbnd._core.constants import AlertSeverity, RunState, TaskRunState
from dbnd._core.errors.base import DatabandApiError
from dbnd._vendor import click
from dbnd.api.alerts import create_alert, delete_alerts_filtered, list_job_alerts


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
    @with_fast_dbnd_context
    @click.option(
        "--value", "-v", help=value_help, type=value_type, required=True,
    )
    @click.pass_context
    def inner_function(ctx, value, metric_name=metric, op=operator):
        # xor is less readable
        if not with_task and ctx.obj.get("task"):
            logger.error("Task is set but it is unavailable for this alert")
            return
        if with_task and not ctx.obj.get("task"):
            logger.error("Task name is required for this alert")
            return

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


@alerts.group()
@click.option("--severity", "-s", help="Alert severity", type=Severity(), required=True)
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
@click.option("--task", "-t", help="Task name", type=click.STRING, required=False)
@click.pass_context
def create(ctx, job, severity, task):
    """Manage alerts for given job."""
    ctx.obj = {"job": job, "severity": severity, "task": task}


# Registering the manage commands
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
        logger.error(e)
    else:
        logger.info(
            "{amount} Alerts configured for {job}:".format(
                job=job, amount=len(alerts_list)
            )
        )
        for alert in alerts_list:
            show_alert(alert)


def show_alert(alert):
    alert.setdefault("value", "")
    try:
        logger.info(
            "user_metric: {user_metric}\t| alert_type: {type} {operator} {value}\t| severity: {severity}".format(
                **alert
            )
        )
    except KeyError:
        logger.error("error while parsing alert {alert}".format(alert=alert))


def cmd_create_alert(manage_ctx, alert, operator, value, user_metric):
    try:
        alert_def_uid = create_alert(
            job_name=manage_ctx.obj["job"],
            task_name=manage_ctx.obj.get("task", None),
            alert_class=alert,
            severity=manage_ctx.obj["severity"],
            operator=operator,
            value=value,
            user_metric=user_metric,
        )
    except DatabandApiError as e:
        logger.error("failed with - {}".format(e.response))
    else:
        logger.info("Created alert %s", alert_def_uid["uid"])


@alerts.command()
@with_fast_dbnd_context
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
@click.option(
    "--alert",
    "-a",
    help="Alert name",
    type=click.Choice(
        [alerts_dict["alert"] for alerts_dict in supported_alerts.values()],
        case_sensitive=False,
    ),
    required=True,
)
def delete(job, alert):
    """Delete alert from the given job"""
    try:
        delete_alerts_filtered(job, alert)
        logger.info("Alert %s deleted from scheduled job %s", alert, job)
    except LookupError as e:
        logger.error(e)
