import logging

from sqlalchemy.exc import IntegrityError

from dbnd._core.constants import RunState
from dbnd._core.tracking.airflow_sync.schemas import (
    ExportData,
    SaveTaskRunLog,
    SetRunStateArgs,
)

logger = logging.getLogger(__name__)


def do_import_data(result):
    # type: (ExportData) -> None
    from dbnd_web.services.tracking_db_service import TrackingDbService

    tracking_service = TrackingDbService()
    for scheduled_job in result.scheduled_job_infos:
        try:
            tracking_service.init_scheduled_job(scheduled_job)
        except Exception as e:
            log_f = logger.exception
            if isinstance(e, IntegrityError) and "UNIQUE" in str(e):
                # assuming already exists
                log_f = logger.warning
            log_f("Failed init_scheduled_job for {}".format(scheduled_job.name))

    for init_args in result.init_args:
        try:
            tracking_service.init_run(init_args)
        except Exception as e:
            log_f = logger.exception
            if isinstance(e, IntegrityError) and "UNIQUE" in str(e):
                # assuming already exists
                log_f = logger.warning
            log_f("Failed init_run for {}".format(init_args.new_run_info))

    # for task_run_attempt_updates in all_updates:
    tracking_service.update_task_run_attempts(result.task_run_attempt_updates)

    for log in result.logs:  # type: SaveTaskRunLog
        tracking_service.save_task_run_log(
            task_run_attempt_uid=log.task_run_attempt_uid, log_body=log.log_body
        )

    for run_state in result.run_states:  # type: SetRunStateArgs
        tracking_service.set_run_state(
            run_uid=run_state.run_uid,
            state=RunState(run_state.state),
            timestamp=run_state.timestamp,
        )
