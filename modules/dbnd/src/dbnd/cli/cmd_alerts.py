from __future__ import print_function

import logging

from dbnd._core.cli.utils import with_fast_dbnd_context
from dbnd._core.constants import AlertSeverity, RunState
from dbnd._vendor import click
from dbnd.api.alerts import delete_alerts, get_alerts_filtered, upsert_alert
from dbnd.api.scheduler import get_scheduled_job_by_name


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
            choices=[s for s in dir(RunState) if not s.startswith("_")],
            case_sensitive=False,
        )

    def convert(self, value, param, ctx):
        return super(RunStateChoice, self).convert(value.upper(), param, ctx)


@click.group()
@click.pass_context
def alerts(ctx):
    """Manage Databand alerts"""
    pass


@alerts.command("list")
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
@with_fast_dbnd_context
def list_jobs(job):
    """Display alert definitions for given job"""
    scheduled_job = get_scheduled_job_by_name(job)
    if scheduled_job:
        alerts_list = get_alerts_filtered(scheduled_job_uid=scheduled_job["uid"])
        logger.info(
            "{amount} Alerts configured for {job}:".format(
                job=job, amount=len(alerts_list)
            )
        )
        for alert in alerts_list:
            logger.info(show_alert(alert))
    else:
        logger.error("No scheduled job found name {job}".format(job=job))


def show_alert(alert):
    return "user_metric: {user_metric}\t| alert_type: {type} {operator} {value}\t| severity: {severity}".format(
        **alert
    )


@alerts.group()
def manage():
    """Manage alerts for given job."""
    pass


@manage.command()
@with_fast_dbnd_context
@click.option("--severity", "-s", help="Alert severity", type=Severity(), required=True)
@click.option(
    "--threshold",
    "-t",
    help="Maximum duration of job run in seconds",
    type=click.INT,
    required=True,
)
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
def maximum_duration(severity, threshold, job):
    """Create/update maximum duration alert. The alert triggers when job run time is more than specified threshold."""

    upsert_alert(
        "RunDurationAlert",
        scheduled_job_name=job,
        severity=severity,
        operator=">",
        value=threshold,
        label="Run Duration (seconds)",
    )


@manage.command()
@with_fast_dbnd_context
@click.option("--severity", "-s", help="Alert severity", type=Severity(), required=True)
@click.option(
    "--threshold", "-t", help="Maximum retries of job", type=click.INT, required=True
)
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
def maximum_retries(severity, threshold, job):
    """Create/update maximum job retries alert. The alert triggers when job retry count hits specified threshold."""

    upsert_alert(
        "MaxRunRetries",
        scheduled_job_name=job,
        severity=severity,
        operator=">",
        value=threshold,
        label="Number of Retries Per Run",
    )


@manage.command()
@with_fast_dbnd_context
@click.option("--severity", "-s", help="Alert severity", type=Severity(), required=True)
@click.option(
    "--threshold",
    "-t",
    help="Duration in seconds of missing success run from job",
    type=click.INT,
    required=True,
)
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
def ran_last_x_seconds(severity, threshold, job):
    """Create/update maximum ran for last X seconds alert.
    It triggers when job is not running successfully after specified period of time"""

    upsert_alert(
        "RanLastXSecondsAlert",
        scheduled_job_name=job,
        severity=severity,
        operator=">",
        value=threshold,
        label="Delay Between Subsequent Runs (seconds)",
    )


@manage.command()
@with_fast_dbnd_context
@click.option("--severity", "-s", help="Alert severity", type=Severity(), required=True)
@click.option(
    "--state", help="State of the run to alert on", type=RunStateChoice(), required=True
)
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
def run_state(severity, state, job):
    """Create/update maximum ran for last X seconds alert.
    It triggers when job is not running successfully after specified period of time"""

    upsert_alert(
        "RunStateAlert",
        scheduled_job_name=job,
        severity=severity,
        operator="==",
        value=state,
        label="Run State",
    )


@alerts.command()
@with_fast_dbnd_context
@click.option("--job", "-j", help="Job name", type=click.STRING, required=True)
@click.option(
    "--alert",
    "-a",
    help="Alert name",
    type=click.Choice(
        ["RunDurationAlert", "MaxRunRetries", "RanLastXSecondsAlert"],
        case_sensitive=False,
    ),
    required=True,
)
def delete(job, alert):
    """Delete alert from the given job"""
    scheduled_job = get_scheduled_job_by_name(job_name=job)
    if scheduled_job:
        alerts = get_alerts_filtered(
            scheduled_job_uid=scheduled_job["uid"], alert_type=alert
        )
        if alerts:
            uids = [alert["uid"] for alert in alerts]
            delete_alerts(uids)
            logger.info("Alert %s deleted from scheduled job %s", alert, job)
        else:
            logger.error("Alert %s is not configured for scheduled job %s", alert, job)
    else:
        logger.error("No scheduled job found name {job}".format(job=job))
