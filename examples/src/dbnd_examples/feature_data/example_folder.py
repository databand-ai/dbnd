import logging

from pandas import DataFrame

from dbnd import output, parameter, task
from targets import DataTarget, Target
from targets.types import Path


logger = logging.getLogger(__name__)


@task(some_output=output.folder)
def plot_graphs(data, some_output):
    # type: (DataFrame, DataTarget) -> DataFrame
    some_output.partition().dump(data)
    some_output.mark_success()
    return data.tail(1)


@task(some_output=output.folder)
def plot_graphs_only(data, some_output):
    # type: (DataFrame, DataTarget) -> None
    some_output.partition().dump(data)


@task(folder_input=parameter.folder[Target])
def read_folder_status(folder_input):
    # type: (Target) -> str
    return str(folder_input.list_partitions())


@task(folder_input=parameter.folder.with_flag("MY_SUCCESS")[Path])
def read_folder_status_custom_flag(folder_input):
    # type: (Path) -> str
    v = str(list(folder_input.glob("*")))
    print(v)
    return v


@task(folder_input=parameter.folder.with_flag(False)[str])
def read_folder_status_noflag(folder_input):
    # type: (str) -> str
    v = str(list(Path(folder_input).glob("*")))
    print(v)
    return v
