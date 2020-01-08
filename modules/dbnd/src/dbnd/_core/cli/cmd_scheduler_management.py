from __future__ import print_function

from functools import update_wrapper

from dbnd._vendor import click
from dbnd._vendor.click import get_current_context
from dbnd._vendor.click_tzdatetime import TZAwareDateTime


SCHEDULED_JOB_HEADERS = [
    "name",
    "active",
    "cmd",
    "schedule_interval",
    "readable_schedule_interval",
    "last_job_date",
    "next_job_date",
]

SCHEDULED_JOB_VERBOSE_HEADERS = SCHEDULED_JOB_HEADERS + [
    "start_date",
    "end_date",
    "catchup",
    "depends_on_past",
    "retries",
    "create_user",
    "create_time",
    "update_user",
    "update_time",
]


datetime_formats = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%SZ%z",
]


def pass_service(f):
    @click.pass_context
    def new_func(ctx, *args, **kwargs):
        return ctx.invoke(f, ctx.obj["scheduled_job_service"], *args, **kwargs)

    return update_wrapper(new_func, f)


@click.group()
@click.pass_context
def schedule(ctx):
    """Manage scheduled jobs"""
    ctx.obj = {}
    ctx.obj["headers"] = SCHEDULED_JOB_HEADERS
    from dbnd import new_dbnd_context

    context = new_dbnd_context(
        autoload_modules=False, conf={"core": {"tracker": ""}}
    ).__enter__()

    ctx.obj["scheduled_job_service"] = context.scheduled_job_service


@schedule.command()
@click.option(
    "--all", "-a", "include_all", is_flag=True, help="Lists all deleted scheduled jobs"
)
@click.option("--verbose", "-v", is_flag=True, help="Print extra job details")
@pass_service
@click.pass_context
def list(ctx, scheduled_job_service, include_all, verbose):
    """List scheduled jobs"""
    scheduled_jobs = scheduled_job_service.get_scheduled_jobs(
        include_deleted=include_all
    )

    ctx.obj["headers"] = (
        SCHEDULED_JOB_VERBOSE_HEADERS if verbose else SCHEDULED_JOB_HEADERS
    )
    _click_echo_jobs(scheduled_jobs, scheduled_job_service)


@schedule.command()
@click.option("--name", "-n", help="Name of the scheduled job to enable)")
@pass_service
def enable(scheduled_job_service, name):
    """Enable scheduled job"""
    scheduled_job_service.set_active(name, True)

    click.echo('Scheduled job "%s" is enabled' % name)


@schedule.command()
@click.option("--name", "-n", help="Name of the scheduled job to pause)")
@pass_service
def pause(scheduled_job_service, name):
    """Pause scheduled job"""
    scheduled_job_service.set_active(name, False)

    click.echo('Scheduled job "%s" paused' % name)


@schedule.command()
@click.option("--name", "-n", help="Name of the scheduled job to undelete)")
@pass_service
def undelete(scheduled_job_service, name):
    """Un-Delete deleted scheduled job"""
    scheduled_job_service.delete_scheduled_job(name, revert=True)


@schedule.command()
@click.option("--name", "-n", help="Name of the scheduled job to delete)")
@click.option("--force", "-f", is_flag=True, help="Delete without confirmation")
@pass_service
def delete(scheduled_job_service, name, force):
    """Delete scheduled job"""
    if not force:
        to_delete = scheduled_job_service.get_scheduled_jobs(name_pattern=name)
        if not to_delete:
            click.echo("no jobs found matching the given name pattern")
            return

        click.echo("the following jobs will be deleted:")
        _click_echo_jobs(to_delete, scheduled_job_service)

        click.confirm("are you sure?", abort=True)

    scheduled_job_service.delete_scheduled_job(name)


@schedule.command()
@click.option("--name", "-n", help="Name of the scheduled job (must be unique)")
@click.option("--cmd", "-c", help="Shell command to run")
@click.option(
    "--schedule-interval",
    "-si",
    help="Any valid cron expression or one of the presets: "
    "@once, @hourly, @daily, @weekly, @monthly or @yearly. "
    + "See documentation for precise definitions of the presets",
)
@click.option("--start-date", "-s", type=TZAwareDateTime(formats=datetime_formats))
@click.option("--end-date", "-ed", type=TZAwareDateTime(formats=datetime_formats))
@click.option(
    "--catchup",
    "-cu",
    type=bool,
    help="Run scheduled past tasks that have not been run. For example: a pipeline is scheduled to run every day"
    + "but the scheduler didn't run for a week. This will cause a catchup run to be scheduled for every day missed. "
    + "Default is determined by the catchup_by_default config flag.",
)
@click.option(
    "--depends-on-past",
    "-dp",
    type=bool,
    help="Only run a job if it's previous run succeeded",
)
@click.option(
    "--retries",
    "-r",
    type=int,
    help="Number of times to retry a failed run. If unset will use the default set in the config file",
)
@click.option(
    "--update",
    "-u",
    is_flag=True,
    help="If a job with the given name already exists partially update it with the given params "
    "(unspecified params will be left as is)",
)
@pass_service
def job(
    scheduled_job_service,
    name,
    cmd,
    schedule_interval,
    start_date,
    end_date,
    catchup,
    depends_on_past,
    update,
    retries,
):
    """Manage scheduled jobs"""
    from dbnd._core.utils.timezone import make_aware, is_localized

    if start_date and not is_localized(start_date):
        start_date = make_aware(start_date)
    if end_date and not is_localized(end_date):
        end_date = make_aware(end_date)
    scheduled_job = {
        "name": name,
        "cmd": cmd,
        "start_date": start_date,
        "end_date": end_date,
        "schedule_interval": schedule_interval,
        "catchup": catchup,
        "depends_on_past": depends_on_past,
        "retries": retries,
    }

    if update:
        res = scheduled_job_service.patch_scheduled_job(scheduled_job)
    else:
        res = scheduled_job_service.post_scheduled_job(scheduled_job)

    _click_echo_jobs([res], scheduled_job_service)


def _click_echo_jobs(jobs, scheduled_job_service):
    from dbnd._core.utils.object_utils import tabulate_objects

    headers = get_current_context().obj["headers"]

    job_objects = [
        scheduled_job_service.ScheduledJobNamedTuple(
            **dict(j.pop("DbndScheduledJob"), **j)
        )
        for j in jobs
    ]

    click.echo(tabulate_objects(job_objects, headers=headers))
