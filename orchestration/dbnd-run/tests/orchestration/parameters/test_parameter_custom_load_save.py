# © Copyright Databand.ai, an IBM Company 2022

import logging

from pandas import DataFrame

from dbnd import parameter, task
from targets import target
from targets.target_config import FileFormat


logger = logging.getLogger(__name__)


@task(result=parameter.save_options(FileFormat.csv, header=False)[DataFrame])
def read_first_lines(lines=parameter.load_options(FileFormat.csv, nrows=10)[DataFrame]):
    assert lines.shape[0] == 10
    return lines


class TestTaskInMemoryParameters(object):
    def test_load_options(self, tmpdir):
        t = target(tmpdir / "file_with_lines.csv")
        t.write("\n".join(str(r) for r in range(20)))

        r = read_first_lines.dbnd_run(lines=t)
        actual = r.task.result.load(DataFrame)

        # we skip the header, so the next value is the header
        logger.info("actual : %s", actual)
        assert actual.columns[0] == "1"
