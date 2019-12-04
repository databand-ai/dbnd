import logging

from dbnd._core.tracking import tracking_store


logger = logging.getLogger(__name__)


class TbSummaryFileStore(tracking_store.TrackingStore):
    def init_run(self, run):
        from dbnd._core.tools.tensorboard import save_task_graph

        save_task_graph(
            run.task,
            comment=run.run_id,
            log_dir=run.local_driver_root.folder("tensorboard.summary"),
        )
