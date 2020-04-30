"""
This is the example that shows how to write some data in your custom file format/reuse some known format

We want to save 2 data frames into HDF5 File store using custom API

1. We create a Marshaller that can read/write to HDF5 using our type (MyData)
2. We register the Marshaller
3. We register the Parameter, so we can use MyData type in classe/functions,

If you have function with MyData - >  it will be converted into  parameter that were used in register_custom_parameter
"""
from __future__ import absolute_import

import logging

from typing import NamedTuple

import pandas as pd

from pandas import DataFrame

from dbnd import PipelineTask, PythonTask, output, parameter, task
from dbnd._core.parameter import register_custom_parameter
from dbnd._core.utils.structures import combine_mappings
from targets.marshalling import register_marshaller
from targets.marshalling.pandas import DataFrameToHdf5
from targets.target_config import FileFormat


logger = logging.getLogger(__name__)
MyData = NamedTuple("MyData", [("features", DataFrame), ("targets", DataFrame)])


class MyDataToHdf5(DataFrameToHdf5):
    def _pd_read(self, *args, **kwargs):
        features = super(MyDataToHdf5, self)._pd_read(*args, key="features", **kwargs)
        targets = super(MyDataToHdf5, self)._pd_read(*args, key="targets", **kwargs)
        return MyData(features=features, targets=targets)

    def _pd_to(self, data, file_or_path, *args, **kwargs):
        kwargs = combine_mappings({"format": "fixed"}, kwargs)
        with pd.HDFStore(file_or_path, "w") as store:
            kwargs.pop("mode", None)
            store.put("features", data.features, data_columns=True, **kwargs)
            store.put("targets", data.targets, data_columns=True, **kwargs)


register_marshaller(MyData, FileFormat.hdf5, MyDataToHdf5())
MyDataParameter = register_custom_parameter(MyData, parameter.data.type(MyData))


class MyDataReport(PythonTask):
    my_data = parameter[MyData]
    report = output[DataFrame]

    def run(self):
        self.report = self.my_data.features.head(1)


class BuildMyData(PythonTask):
    my_data = output.hdf5[MyData]

    def run(self):
        features = pd.DataFrame(data=[[1, 2], [2, 3]], columns=["Names", "Births"])
        targets = pd.DataFrame(data=[[1, 22], [2, 33]], columns=["Names", "Class"])
        self.my_data = MyData(features=features, targets=targets)


@task
def validate_my_data(my_data):
    # type: (MyData)->bool
    return not my_data.features.empty


class MyHdf5DataPipeline(PipelineTask):
    my_data = output[MyData]
    report = output
    validated = output

    def band(self):
        self.my_data = BuildMyData().my_data
        self.validated = validate_my_data(self.my_data)
        self.report = MyDataReport(my_data=self.my_data).report
