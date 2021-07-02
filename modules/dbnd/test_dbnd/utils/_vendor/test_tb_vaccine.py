import inspect
import logging
import sys

from pytest import fixture

from dbnd._vendor.tbvaccine import TBVaccine
from dbnd._core.errors.errors_utils import UserCodeDetector
from test_dbnd.helpers import raise_example_failure


raise_example_dir = inspect.getfile(raise_example_failure).replace(".pyc", ".py")
current_test = __file__.replace(".pyc", ".py")


class TestTBVaccine(object):
    @fixture
    def user_code_detector(self):
        return UserCodeDetector(
            system_code_dirs=[current_test], code_dir=raise_example_dir
        )

    def test_simple_skip(self, user_code_detector):
        try:
            raise_example_failure("test")
        except Exception:
            tbvaccine = TBVaccine(
                user_code_detector=user_code_detector, skip_non_user_on_isolate=True
            )
            exception_message = tbvaccine.format_tb(*sys.exc_info())
            print(exception_message)

            assert "test_simple_skip" not in exception_message

    def test_no_skip(self, user_code_detector):
        try:
            raise_example_failure("test")
        except Exception:
            tbvaccine = TBVaccine(
                user_code_detector=user_code_detector, skip_non_user_on_isolate=False
            )
            exception_message = tbvaccine.format_tb(*sys.exc_info())
            print(exception_message)

            assert "test_no_skip" in exception_message
            assert "\x1b" in exception_message

    def test_no_colors(self, user_code_detector):
        try:
            raise_example_failure("test")
        except Exception:
            tbvaccine = TBVaccine(
                user_code_detector=user_code_detector,
                skip_non_user_on_isolate=False,
                no_colors=True,
            )
            exception_message = tbvaccine.format_tb(*sys.exc_info())
            print(exception_message)

            assert "test_simple" not in exception_message
            assert "\x1b" not in exception_message

    def test_windows_stack(self):
        stack = r"""
Traceback (most recent call last):
  File "C:\Users\Databand\Anaconda2\lib\site-packages\dbnd_airflow\simple_executor.py", line 62, in sync
    self._run_task_instance(command[0], **command[1])
  File "C:\Users\Databand\Anaconda2\lib\site-packages\airflow\utils\db.py", line 74, in wrapper
    return func(*args, **kwargs)
  File "C:\Users\Databand\Anaconda2\lib\site-packages\dbnd_airflow\simple_executor.py", line 142, in _run_task_instance
    ti._run_raw_task(mark_success=mark_success, job_id=ti.job_id, pool=pool)
  File "C:\Users\Databand\Anaconda2\lib\site-packages\airflow\utils\db.py", line 74, in wrapper
    return func(*args, **kwargs)
  File "C:\Users\Databand\Anaconda2\lib\site-packages\dbnd_web\models\dbnd_task_instance.py", line 226, in _run_raw_task
    result = databand_execute(task_copy, context=context)
  File "C:\Users\Databand\Anaconda2\lib\site-packages\dbnd_web\models\dbnd_task_instance.py", line 39, in databand_execute
    return airflow_operator.task._task_scheduler_execute(airflow_context=context)
  File "C:\Users\Databand\Anaconda2\lib\site-packages\databand\core\task\task.py", line 171, in _task_scheduler_execute
    return self.ctrl.task_runner.execute(airflow_context)
  File "C:\Users\Databand\Anaconda2\lib\site-packages\databand\core\task_ctrl\task_runner.py", line 47, in execute
    return self._execute()
  File "C:\Users\Databand\Anaconda2\lib\site-packages\databand\core\task_ctrl\task_runner.py", line 97, in _execute
    result = self.task._task_submit()
  File "C:\Users\Databand\Anaconda2\lib\site-packages\databand\core\task\task.py", line 182, in _task_submit
    return self._task_run()
  File "C:\Users\Databand\Anaconda2\lib\site-packages\databand\core\task\task.py", line 188, in _task_run
    return self.run()
  File "C:\Users\Databand\Anaconda2\lib\site-packages\databand\core\decorator\func_task.py", line 48, in run
    self._invoke_func()
  File "C:\Users\Databand\Anaconda2\lib\site-packages\databand\core\decorator\func_task.py", line 28, in _invoke_func
    result = func_call(**invoke_kwargs)
  File "C:/Users/Databand/Documents/source/user_package/user_code.py", line 42, in user_function
    max, min = find_min_max(data, data)
AttributeError: 'list' object has no attribute 'max'
        """
        user_code_detector = UserCodeDetector(
            system_code_dirs=[r"C:\Users\Databand\Anaconda2\lib\site-packages"],
            code_dir="C:/Users/Databand/Documents/source",
        )
        tbvaccine = TBVaccine(
            user_code_detector=user_code_detector,
            skip_non_user_on_isolate=True,
            isolate=True,
            no_colors=True,
        )
        stack = stack.split("\n")
        tb_print = tbvaccine._new_tb_print()
        tb_print._process(stack)
        exception_message = tb_print._buffer
        logging.warning("Actual exception message: \n%s", exception_message)

        assert "func_task" not in exception_message
        assert "user_code" in exception_message
