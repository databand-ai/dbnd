import logging

from dbnd.tasks import dbnd_doctor
from dbnd.tasks.doctor.system_logging import logging_status


logger = logging.getLogger(__name__)


class TestDoctorTasks(object):
    def test_doctor_logging_status_func(self):
        expected = logging_status()
        logger.info(expected)
        assert expected
        assert "Logging Status" in expected

    def test_doctor_logging_status_task(self):
        dr = logging_status.dbnd_run()
        expected = dr.root_task.result.load(str)
        assert expected
        assert "Logging Status" in expected

    def test_dbnd_doctor_func(self):
        result = dbnd_doctor()
        assert result

    def test_dbnd_doctor_task(self):
        result = dbnd_doctor.dbnd_run()
        assert result
