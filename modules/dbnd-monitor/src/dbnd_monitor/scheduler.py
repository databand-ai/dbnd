# © Copyright Databand.ai, an IBM Company 2024
import logging
import threading
import time

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Set


logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self):
        self.group_pools: dict[str, ThreadPoolExecutor] = defaultdict(
            ThreadPoolExecutor
        )
        self.running_tasks: Set[str] = set()

    def try_schedule_task(self, task: Callable, task_id: str, group: str = "") -> bool:
        if self.is_task_running(task_id):
            return False

        logger.info(
            "Scheduling a new task, task id: %s \n there are currently %s tasks running",
            task_id,
            len(self.running_tasks),
        )
        future = self.group_pools[group].submit(
            self.task_wrapper, task=task, task_id=task_id
        )
        self.running_tasks.add(task_id)
        future.add_done_callback(lambda _: self.handel_finished_task(task_id))
        return True

    def handel_finished_task(self, task_id: str):
        self.running_tasks.discard(task_id)
        logger.info(
            "Scheduled task is finished, task id: %s \n there are currently %s tasks running",
            task_id,
            len(self.running_tasks),
        )

    def is_task_running(self, task_id: str) -> bool:
        return task_id in self.running_tasks

    def wait_all_tasks(self):
        while self.running_tasks:
            time.sleep(1)

    def task_wrapper(self, task: Callable, task_id: str):
        old_thread_name = threading.current_thread().name
        threading.current_thread().name = task_id
        try:
            task()
        finally:
            threading.current_thread().name = old_thread_name
