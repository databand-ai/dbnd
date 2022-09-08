# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging

from dbnd._core.constants import AlertDefOperator, AlertSeverity, RunState, TaskRunState
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
from dbnd.cli.cmd_alerts_constants import (
    ANOMALY_ALERT_LOOK_BACK_HELP,
    ANOMALY_ALERT_SENSITIVITY_HELP,
    CREATE_HELP_MSG,
    DELETE_HELP_MSG,
    IS_STR_VALUE_HELP,
    LIST_HELP_MSG,
    METRIC_HELP,
    RANGE_ALERT_BASE_LINE_HELP,
    RANGE_ALERT_RANGE_HELP,
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
            choices=TaskRunState.all_values(), case_sensitive=False
        )

    def convert(self, value, param, ctx):
        return super(TaskRunStateChoice, self).convert(value.upper(), param, ctx)


operator_to_required_options = {
    AlertDefOperator.RANGE.value: ["baseline", "range"],
    AlertDefOperator.ANOMALY.value: ["look_back", "sensitivity"],
    AlertDefOperator.EQUAL.value: ["value"],
    AlertDefOperator.NOT_EQUAL.value: ["value"],
    AlertDefOperator.GREATER_THAN.value: ["value"],
    AlertDefOperator.NOT_LESS_THAN.value: ["value"],
    AlertDefOperator.LESS_THAN.value: ["value"],
    AlertDefOperator.NOT_GREATER_THAN.value: ["value"],
}

supported_alerts = {
    "run_duration": dict(
        value_help="Run duration of pipeline in seconds",
        value_type=click.INT,
        with_task=False,
        alert_type="RunDuration",
        operator=None,
        metric="Run Duration (seconds)",
        is_str_value=False,
    ),
    "run_state": dict(
        value_help="State of the run to alert on",
        value_type=RunStateChoice(),
        with_task=False,
        alert_type="RunState",
        operator="==",
        metric="Run State",
        is_str_value=False,
    ),
    "ran_last_x_seconds": dict(
        value_help="Duration in seconds of missing success run from pipeline",
        value_type=click.INT,
        with_task=False,
        alert_type="RanLastXSeconds",
        operator=">",
        metric="Delay Between Subsequent Runs (seconds)",
        is_str_value=False,
    ),
    "task_state": dict(
        value_help="State of the task run to alert on",
        value_type=TaskRunStateChoice(),
        with_task=True,
        alert_type="TaskState",
        operator="==",
        metric="State",
        is_str_value=True,
    ),
    "custom_metric": dict(
        value_help="Threshold to match the metric",
        value_type=click.STRING,
        with_task=True,
        alert_type="CustomMetric",
        operator=None,
        metric=None,
        is_str_value=None,
    ),
}


def get_option(ctx, param_name: str, is_parent_command: bool):
    parent_params = (
        ctx.parent.command.params if is_parent_command else ctx.command.params
    )
    return next(op for op in parent_params if op.name == param_name)


def validate_option(
    ctx,
    option_name: str,
    option_value: str,
    option_condition: bool,
    from_parent_command: bool,
):
    if not option_condition and option_value:
        raise click.BadParameter(
            f"'{option_name}' is set but it is unavailable for this alert",
            param=get_option(ctx, option_name, from_parent_command),
            ctx=ctx,
        )
    if option_condition and not option_value:
        raise click.MissingParameter(
            ctx=ctx,
            param=get_option(ctx, option_name, from_parent_command),
            message=f"'{option_name}' is required for this alert",
        )


def validate_passed_options(ctx, with_task, operator, str_value, **more_options):
    validate_option(ctx, "task", ctx.obj.get("task"), with_task, True)
    # support only equal\not equal operator with task of str value
    if (
        with_task
        and str_value
        and operator
        not in [AlertDefOperator.EQUAL.value, AlertDefOperator.NOT_EQUAL.value]
    ):
        raise click.BadArgumentUsage(
            f"Metrics with string values can be operated only with '==' and '!=', "
            f"But operator '{operator}' was supplied.",
            ctx=ctx,
        )
    for option_name, option_value in more_options.items():
        is_required_option = option_name in operator_to_required_options[operator]
        validate_option(ctx, option_name, option_value, is_required_option, False)


def bind_create_to_supported_alert(
    name, alert_type, with_task, value_help, value_type, operator, metric, is_str_value
):
    @click.option("--value", "-v", help=value_help, type=value_type, required=False)
    @click.option(
        "--baseline",
        "-bl",
        help=RANGE_ALERT_BASE_LINE_HELP,
        type=click.FLOAT,
        required=False,
    )
    @click.option(
        "--range", "-r", help=RANGE_ALERT_RANGE_HELP, type=click.FLOAT, required=False
    )
    @click.option(
        "--look-back",
        "-lb",
        help=ANOMALY_ALERT_LOOK_BACK_HELP,
        type=click.INT,
        required=False,
    )
    @click.option(
        "--sensitivity",
        "-sn",
        help=ANOMALY_ALERT_SENSITIVITY_HELP,
        type=click.INT,
        required=False,
    )
    @click.pass_context
    def create_alert_command(
        ctx,
        value,
        baseline,
        range,
        look_back,
        sensitivity,
        metric_name=metric,
        op=operator,
        str_value=is_str_value,
    ):
        validate_passed_options(
            ctx,
            with_task,
            op,
            str_value,
            value=value,
            baseline=baseline,
            range=range,
            look_back=look_back,
            sensitivity=sensitivity,
        )
        more_options = {
            "baseline": baseline,
            "range": range,
            "look_back": look_back,
            "sensitivity": sensitivity,
        }
        cmd_create_alert(
            manage_ctx=ctx,
            alert_type=alert_type,
            operator=op,
            value=value,
            user_metric=metric_name,
            str_value=str_value,
            **more_options,
        )

    if operator is None:
        create_alert_command = click.option(
            "--op",
            "-o",
            help="your operator",
            type=click.Choice(AlertDefOperator.all_values()),
            required=True,
        )(create_alert_command)

    if metric is None:
        create_alert_command = click.option(
            "--metric-name", "-m", help=METRIC_HELP, type=click.STRING, required=True
        )(create_alert_command)

    if is_str_value is None:
        create_alert_command = click.option(
            "--str-value", help=IS_STR_VALUE_HELP, type=click.BOOL, required=True
        )(create_alert_command)

    create_alert_command.__name__ = name
    return create_alert_command


@click.group()
@click.pass_context
def alerts(ctx):
    """Manage Databand alerts"""


@alerts.group(help=CREATE_HELP_MSG)
@click.option("--severity", "-s", help="Alert severity", type=Severity(), required=True)
@click.option(
    "--pipeline", "-p", help="Pipeline name", type=click.STRING, required=False
)
@click.option("--pipeline-id", type=click.STRING, required=False)
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
def create(ctx, pipeline, pipeline_id, severity, task, uid):
    """Create or update alerts for given job."""
    ctx.obj = {
        "job": pipeline,
        "job_id": pipeline_id,
        "severity": severity,
        "task": task,
        "uid": uid,
    }


@alerts.command("list", help=LIST_HELP_MSG)
@click.option(
    "--pipeline", "-p", help="Pipeline name", type=click.STRING, required=True
)
def list_jobs(pipeline):
    try:
        alerts_list = list_job_alerts(job_name=pipeline)
    except LookupError as e:
        logger.warning(e)
    else:
        print_table("Alerts configured for {job}".format(job=pipeline), alerts_list)
        alert_uids = [alert["uid"] for alert in alerts_list]
        logger.info("Alerts listed: %s", str(alert_uids))


@alerts.command(help=DELETE_HELP_MSG)
@options_dependency("pipeline", "name")
@required_mutually_exclusive_options("uid", "wipe", "pipeline")
@click.option("--uid", "-u", help="alert uid", type=click.STRING)
@click.option("--wipe", help="delete all alerts", is_flag=True)
@click.option("--pipeline", "-p", help="pipeline name", type=click.STRING)
@click.option("--name", "-n", help="alert custom name", type=click.STRING)
def delete(uid, wipe, pipeline, name):
    if wipe:
        alerts_list = get_alerts_filtered()
    elif uid:
        alerts_list = get_alerts_filtered(alert_def_uid=uid)
    else:
        alerts_list = get_alerts_filtered(job_name=pipeline, custom_name=name)
    if alerts_list:
        uids = [alert["uid"] for alert in alerts_list]
        delete_alerts(uids)
        print_table("deleted", alerts_list)
        logger.info("Deleted uids: %s", str(uids))
    else:
        logger.warning("Alerts not found to delete")


def print_table(header, alerts_list):
    banner = TextBanner(header)
    banner.write(build_alerts_table(alerts_list))
    logger.info(banner.get_banner_str())


def build_alerts_table(alerts_data):
    extract_keys = (
        "uid",
        "job_name",
        "task_name",
        "task_repr",
        "custom_name",
        "severity",
        "type",
        "operator",
        "value",
        "user_metric",
    )
    headers = (
        "uid",
        "pipeline",
        "task",
        "task_repr",
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


def assign_operator_to_alert_type(base_alert_type, operator):
    alert_type = base_alert_type
    if operator == AlertDefOperator.ANOMALY.value:
        alert_type = "ML" + alert_type
    if operator == AlertDefOperator.RANGE.value:
        alert_type += "Range"
    alert_type += "Alert"
    return alert_type


def cmd_create_alert(
    manage_ctx, alert_type, operator, value, user_metric, str_value, **more_options
):
    full_alert_type = assign_operator_to_alert_type(alert_type, operator)
    task = manage_ctx.obj.get("task", None)  # Currently used in task_name & task_repr
    try:
        alert_def_uid = create_alert(
            job_name=manage_ctx.obj.get("job"),
            job_id=manage_ctx.obj.get("job_id"),
            task_name=task,
            task_repr=task,
            uid=manage_ctx.obj.get("uid", None),
            alert_class=full_alert_type,
            severity=manage_ctx.obj["severity"],
            operator=operator,
            value=value,
            user_metric=user_metric,
            is_str_value=str_value,
            **more_options,
        )
    except DatabandApiError as e:
        logger.warning("failed with - %s", e.response)
    else:
        logger.info("Created alert %s", alert_def_uid["uid"])


def register_create_command_to_alerts():
    # Registering the create commands
    for name, values in supported_alerts.items():
        binder_to_sub_command = bind_create_to_supported_alert(name, **values)
        create.command(name=name.replace("_", "-"))(binder_to_sub_command)


register_create_command_to_alerts()
