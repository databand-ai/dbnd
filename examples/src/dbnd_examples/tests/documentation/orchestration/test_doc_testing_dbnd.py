from dbnd import dbnd_run_cmd, task
from dbnd.errors import DatabandRunError
from dbnd.testing.helpers_pytest import run_locally__raises


@task
def task_raises_exception():
    raise DatabandRunError


@task
def calculate_alpha(alpha=0.5):
    return alpha


class TestDocTestingDBND:
    def test_doc(self):
        #### DOC START
        alpha = calculate_alpha.dbnd_run(task_env="local", task_version="now")
        #### DOC END

        #### DOC START
        ####alpha = dbnd_run_cmd(["calculate_alpha", "-r alpha=0.4"])
        alpha = dbnd_run_cmd(
            [
                "dbnd_examples.tests.documentation.orchestration.test_doc_testing_dbnd.calculate_alpha",
                "-r alpha=0.4",
            ]
        )
        #### DOC END

        #### DOC START

        pytest_plugins = [
            "dbnd.testing.pytest_dbnd_plugin",
        ]
        #### DOC END

    #### DOC START
    def test_cli_raises_ex(self):
        run_locally__raises(DatabandRunError, ["task_raises_exception"])

    #### DOC END
