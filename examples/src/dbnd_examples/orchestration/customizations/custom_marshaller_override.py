# © Copyright Databand.ai, an IBM Company 2022

"""
Override build in implementation of Hdf5 serialization
"""
import pandas as pd

from dbnd import output, task
from targets.marshalling import DataFrameToHdf5, register_marshaller
from targets.target_config import FileFormat


class DataFrameToHdf5Table(DataFrameToHdf5):
    def _pd_to(self, value, *args, **kwargs):
        # WE WILL CHANGE THE DEFAULT FORMAT FOR MARSHALLER
        kwargs.setdefault("format", "table")
        return super(DataFrameToHdf5Table, self)._pd_to(value, *args, **kwargs)


register_marshaller(pd.DataFrame, FileFormat.hdf5, DataFrameToHdf5Table())


@task(result=output.hdf5)
def dump_as_hdf5_table():
    # type: ()-> pd.DataFrame
    return pd.DataFrame(
        data=list(zip(["Bob", "Jessica"], [968, 155])), columns=["Names", "Births"]
    )
