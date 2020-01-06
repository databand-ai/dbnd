import logging

import pytest

from dbnd._core.cli.main import dbnd_schedule_cmd


logger = logging.getLogger(__name__)


def assert_cmd_output(cmd, output, capsys):
    # fix multiline indent:
    expected_output = output.replace("\n" + " " * 12, "\n").strip()
    dbnd_schedule_cmd(cmd)
    captured = capsys.readouterr()
    assert expected_output == captured.out.strip()


class TestCmdScheduling(object):
    def test_cmd_scheduling_help(self, capsys):
        assert_cmd_output(
            "--help",
            """
            Usage: python [OPTIONS] COMMAND [ARGS]...

              Manage scheduled jobs

            Options:
              --help  Show this message and exit.

            Commands:
              delete    Delete scheduled job
              enable    Enable scheduled job
              job       Manage scheduled jobs
              list      List scheduled jobs
              pause     Pause scheduled job
              undelete  Un-Delete deleted scheduled job
            """,
            capsys,
        )

    @pytest.mark.dbnd_integration
    def test_cmd_scheduling_list(self, capsys):
        assert_cmd_output(
            "list",
            """
            name    active    cmd                                        schedule_interval    readable_schedule_interval    last_job_date    next_job_date
            ------  --------  -----------------------------------------  -------------------  ----------------------------  ---------------  -----------------------------
            touchy  True      touch /tmp/scheduler_test/{{ ts_nodash }}  * * * * *            Every minute                                   Tue, 04 Jan 94 00:02:00 +0000
            """,
            capsys,
        )

    @pytest.mark.dbnd_integration
    def test_cmd_scheduling_enable_and_pause(self, capsys):
        assert_cmd_output(
            "pause --name touchy", 'Scheduled job "touchy" paused', capsys
        )
        assert_cmd_output(
            "list",
            """
            name    active    cmd                                        schedule_interval    readable_schedule_interval    last_job_date    next_job_date
            ------  --------  -----------------------------------------  -------------------  ----------------------------  ---------------  -----------------------------
            touchy  False     touch /tmp/scheduler_test/{{ ts_nodash }}  * * * * *            Every minute                                   Tue, 04 Jan 94 00:02:00 +0000
            """,
            capsys,
        )

        assert_cmd_output(
            "enable --name touchy", 'Scheduled job "touchy" is enabled', capsys
        )
        assert_cmd_output(
            "list",
            """
            name    active    cmd                                        schedule_interval    readable_schedule_interval    last_job_date    next_job_date
            ------  --------  -----------------------------------------  -------------------  ----------------------------  ---------------  -----------------------------
            touchy  True      touch /tmp/scheduler_test/{{ ts_nodash }}  * * * * *            Every minute                                   Tue, 04 Jan 94 00:02:00 +0000
            """,
            capsys,
        )

    @pytest.mark.dbnd_integration
    def test_cmd_scheduling_delete_undelete(self, capsys):
        assert_cmd_output("delete --name touchy --force", "", capsys)

        assert_cmd_output(
            "list",
            """
            name    active    cmd    schedule_interval    readable_schedule_interval    last_job_date    next_job_date
            ------  --------  -----  -------------------  ----------------------------  ---------------  ---------------
            """,
            capsys,
        )

        assert_cmd_output(
            "list --all",
            """
            name    active    cmd                                        schedule_interval    readable_schedule_interval    last_job_date    next_job_date
            ------  --------  -----------------------------------------  -------------------  ----------------------------  ---------------  -----------------------------
            touchy  False     touch /tmp/scheduler_test/{{ ts_nodash }}  * * * * *            Every minute                                   Tue, 04 Jan 94 00:02:00 +0000
            """,
            capsys,
        )

        assert_cmd_output("undelete --name touchy", "", capsys)
        assert_cmd_output(
            "list",
            """
            name    active    cmd                                        schedule_interval    readable_schedule_interval    last_job_date    next_job_date
            ------  --------  -----------------------------------------  -------------------  ----------------------------  ---------------  -----------------------------
            touchy  False     touch /tmp/scheduler_test/{{ ts_nodash }}  * * * * *            Every minute                                   Tue, 04 Jan 94 00:02:00 +0000
            """,
            capsys,
        )
        assert_cmd_output(
            "enable --name touchy", 'Scheduled job "touchy" is enabled', capsys
        )

    @pytest.mark.dbnd_integration
    def test_cmd_schedulingdelete_create(self, capsys):
        assert_cmd_output(
            'job --name hotjob --cmd "touch /tmp/scheduler_test/{{ ts_nodash }}" --start-date 1994-01-04 --schedule-interval "* * * * *" --catchup True',
            """
            name    active    cmd                                        schedule_interval    readable_schedule_interval    last_job_date    next_job_date
            ------  --------  -----------------------------------------  -------------------  ----------------------------  ---------------  -----------------------------
            hotjob  True      touch /tmp/scheduler_test/{{ ts_nodash }}  * * * * *            Every minute                                   Tue, 04 Jan 94 00:00:00 +0000
            """,
            capsys,
        )
        assert_cmd_output(
            "list",
            """
            name    active    cmd                                        schedule_interval    readable_schedule_interval    last_job_date    next_job_date
            ------  --------  -----------------------------------------  -------------------  ----------------------------  ---------------  -----------------------------
            touchy  True      touch /tmp/scheduler_test/{{ ts_nodash }}  * * * * *            Every minute                                   Tue, 04 Jan 94 00:02:00 +0000
            hotjob  True      touch /tmp/scheduler_test/{{ ts_nodash }}  * * * * *            Every minute                                   Tue, 04 Jan 94 00:00:00 +0000
            """,
            capsys,
        )
