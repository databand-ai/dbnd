# © Copyright Databand.ai, an IBM Company 2022

from dbnd import band, task
from targets import target
from targets.types import Path


class _MyObject(object):
    pass


@task
def t_Path(path1, path2):
    # type: (str, str) -> str
    return path1


@band
def t_band_path(path1, path2):
    # type: (Path, Path) -> str
    return t_Path(path1=path1, path2=path2)


@band
def t_band2_path():
    path1 = Path(__file__)
    path2 = target(__file__)

    return t_band_path(path1=path1, path2=path2)


class TestTaskPathlibParameters(object):
    def test_band(self):
        t_band2_path.dbnd_run()
