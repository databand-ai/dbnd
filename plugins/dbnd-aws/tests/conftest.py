# inline conftest
import datetime
import uuid

from pytest import fixture

from dbnd import config


# we need to dbnd module before airflow, otherwise we will not get airflow_bome
pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]


@fixture
def s3_path():

    return "s3://{}/{}/{}".format(
        str(config.get("aws_tests", "bucket_name")),
        datetime.datetime.today().strftime("%Y-%m-%d"),
        str(uuid.uuid4()),
    )
