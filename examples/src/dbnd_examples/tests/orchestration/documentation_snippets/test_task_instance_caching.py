import datetime

from dbnd import PythonTask, parameter


class DateTask(PythonTask):
    date = parameter[datetime.date]


class TestParameters(object):
    def test_task_instance_caching(self):
        a = datetime.date(2014, 1, 21)
        b = datetime.date(2014, 1, 21)

        assert a is not b

        c = DateTask(date=a)
        d = DateTask(date=b)

        assert c is d
