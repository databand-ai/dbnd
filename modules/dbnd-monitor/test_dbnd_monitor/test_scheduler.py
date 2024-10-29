# © Copyright Databand.ai, an IBM Company 2024

from unittest.mock import MagicMock

import pytest

from dbnd_monitor.scheduler import Scheduler


class TestScheduler:
    @pytest.fixture
    def scheduler(self):
        return Scheduler()

    def test_tasks_are_executed_async(self, scheduler):
        # Arrange
        first_task = MagicMock()
        second_task = MagicMock()
        third_task = MagicMock()

        # Act
        scheduler.try_schedule_task(first_task, "1", "group")
        scheduler.try_schedule_task(second_task, "2", "group")
        scheduler.try_schedule_task(third_task, "3", "group")
        scheduler.group_pools["group"].shutdown()

        # Assert
        first_task.assert_called()
        second_task.assert_called()
        third_task.assert_called()

    def test_task_not_running_twice_simultaneously__same_task_id(self, scheduler):
        # Arrange
        scheduler.group_pools["group"].submit = MagicMock()
        func = lambda _: 5
        first_task = func
        second_task = func

        # Act
        scheduler.try_schedule_task(first_task, "1", "group")
        scheduler.try_schedule_task(second_task, "1" "group")

        # Assert
        assert scheduler.group_pools["group"].submit.call_count == 1

    def test_task_removed_from_pipeline__finished_successful(self, scheduler):
        # Arrange
        task = lambda _: 5

        # Act
        scheduler.try_schedule_task(task, "group")

        scheduler.group_pools["group"].shutdown()

        # Assert
        assert len(scheduler.running_tasks) == 0

    def test_task_removed_from_pipeline__finished_with_exception(self, scheduler):
        # Arrange
        task = MagicMock()
        task.side_effect = Exception

        # Act
        scheduler.try_schedule_task(task, "group")

        scheduler.group_pools["group"].shutdown()

        # Assert
        assert len(scheduler.running_tasks) == 0
