"""
This is a way to add another format to serialize/deserialize pandas data frame
"""

import pandas as pd

from dbnd import output, task
from targets.marshalling import register_marshaller
from targets.marshalling.pandas import _PandasMarshaller
from targets.target_config import register_file_extension


# 1. create file extension
excel_file_ext = register_file_extension("xlsx")


class DataFrameToExcel(_PandasMarshaller):
    def _pd_read(self, *args, **kwargs):
        return pd.read_excel(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        return value.to_excel(*args, **kwargs)


# 2. register type to extension mapping
register_marshaller(pd.DataFrame, excel_file_ext, DataFrameToExcel())


@task(result=output(output_ext=excel_file_ext))
def dump_as_excel_table():
    # type: ()-> pd.DataFrame
    df = pd.DataFrame(
        data=list(zip(["Bob", "Jessica"], [968, 155])), columns=["Names", "Births"]
    )
    return df
