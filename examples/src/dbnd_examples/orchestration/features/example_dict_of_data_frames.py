import logging

from typing import Dict

from pandas import DataFrame

from dbnd import output, task


logger = logging.getLogger(__name__)


@task(result=output.folder[Dict[str, DataFrame]])
def create_multiple_data_frames(data):
    # type: (DataFrame) -> Dict[str, DataFrame]

    return {"a": data.head(1), "b": data.head(2), "c": data.head(1)}
