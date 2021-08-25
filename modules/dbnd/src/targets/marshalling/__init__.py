from __future__ import absolute_import

import logging
import typing

import six

from dbnd._core.errors import friendly_error
from targets.marshalling.file import (
    ObjJsonMarshaller,
    ObjPickleMarshaller,
    ObjYamlMarshaller,
    StrLinesMarshaller,
    StrMarshaller,
)
from targets.marshalling.marshaller_ctrl import MarshallerCtrl
from targets.target_config import FileFormat
from targets.types import DataList
from targets.values import get_value_type_of_type
from targets.values.version_value import VersionStr


logger = logging.getLogger(__name__)

MARSHALERS = {}


def register_basic_data_marshallers():
    register_marshallers(
        object,
        {
            FileFormat.txt: StrMarshaller(),
            FileFormat.pickle: ObjPickleMarshaller(),
            FileFormat.json: ObjJsonMarshaller(),
            FileFormat.yaml: ObjYamlMarshaller(),
        },
    )
    register_marshallers(
        typing.List,
        {
            FileFormat.txt: StrLinesMarshaller(),
            FileFormat.csv: StrLinesMarshaller(),
            FileFormat.json: ObjJsonMarshaller(),
            FileFormat.yaml: ObjYamlMarshaller(),
            FileFormat.pickle: ObjPickleMarshaller(),
        },
    )
    register_marshallers(
        str,
        {
            FileFormat.txt: StrMarshaller(),
            FileFormat.csv: StrMarshaller(),
            FileFormat.pickle: ObjPickleMarshaller(),
            FileFormat.json: ObjJsonMarshaller(),
        },
    )

    MARSHALERS[VersionStr] = MARSHALERS[str]

    list_marshalers = MARSHALERS[typing.List]
    for t in [typing.List[object], typing.List[str], DataList, DataList[str]]:
        MARSHALERS[t] = list_marshalers

    try:
        import numpy
        from targets.marshalling.numpy import (
            NumpyArrayMarshaller,
            NumpyArrayPickleMarshaler,
        )
    except ImportError:
        pass

    try:
        import numpy as np
        import pandas as pd

        from targets.marshalling.pandas import (
            DataFrameDictToHdf5,
            DataFrameToCsv,
            DataFrameToExcel,
            DataFrameToFeather,
            DataFrameToHdf5,
            DataFrameToJson,
            DataFrameToParquet,
            DataFrameToPickle,
            DataFrameToTable,
            DataFrameToTsv,
        )

        register_marshallers(
            pd.DataFrame,
            {
                FileFormat.txt: DataFrameToCsv(),
                FileFormat.csv: DataFrameToCsv(),
                FileFormat.table: DataFrameToTable(),
                FileFormat.parquet: DataFrameToParquet(),
                FileFormat.feather: DataFrameToFeather(),
                FileFormat.hdf5: DataFrameToHdf5(),
                FileFormat.pickle: DataFrameToPickle(),
                FileFormat.json: DataFrameToJson(),
                FileFormat.tsv: DataFrameToTsv(),
                FileFormat.excel: DataFrameToExcel(),
            },
        )
        register_marshallers(
            pd.Series,
            {
                FileFormat.csv: DataFrameToCsv(series=True),
                FileFormat.table: DataFrameToTable(),
                FileFormat.parquet: DataFrameToParquet(),
                FileFormat.feather: DataFrameToFeather(),
                FileFormat.hdf5: DataFrameToHdf5(),
                FileFormat.pickle: DataFrameToPickle(),
                FileFormat.json: DataFrameToJson(),
            },
        )
        register_marshallers(
            np.ndarray,
            {
                FileFormat.numpy: NumpyArrayMarshaller(),
                FileFormat.pickle: NumpyArrayPickleMarshaler(),
            },
        )
        register_marshallers(
            typing.Dict[str, pd.DataFrame], {FileFormat.hdf5: DataFrameDictToHdf5()}
        )
    except ImportError:
        pass
    try:
        from matplotlib import figure
        from targets.marshalling.matplotlib import MatplotlibFigureMarshaller

        register_marshallers(
            figure.Figure,
            {
                FileFormat.png: MatplotlibFigureMarshaller(),
                FileFormat.pdf: MatplotlibFigureMarshaller(),
                FileFormat.pickle: ObjPickleMarshaller(),
            },
        )
    except ImportError:
        pass


def _marshaller_options_message(value_type, value_options, object_options):
    value_options = set(value_options.keys())
    object_options = set(object_options.keys())
    object_options.difference_update(value_options)
    msg = "For '{type}' you can use {value_options}.".format(
        value_options=",".join(sorted(value_options)), type=value_type.type
    )
    if object_options:
        msg += (
            "Default marshallers (Type[object]) "
            "will be used for formats {object_options}".format(
                object_options=",".join(sorted(object_options))
            )
        )
    return msg


def get_marshaller_ctrl(target, value_type, config=None):
    config = config or target.config

    value_type = get_value_type_of_type(value_type, inline_value_type=True)
    marshaller_options = MARSHALERS.get(value_type.type, {})
    object_marshaller_options = MARSHALERS.get(object)

    target_file_format = config.format
    if target_file_format is None:
        target_file_format = FileFormat.txt
        logger.warning("Can not find file type of '%s', assuming it's .txt", target)
        # raise friendly_error.targets.no_format(
        #     target=target,
        #     options_message=_marshaller_options_message(
        #         value_type=value_type,
        #         value_options=marshaller_options,
        #         object_options=object_marshaller_options,
        #     ),
        # )

    marshaller = marshaller_options.get(target_file_format)
    if not marshaller:
        # let's try to check marshallers for object
        marshaller = object_marshaller_options.get(target_file_format)
        if not marshaller:
            raise friendly_error.targets.no_marshaller(
                target=target,
                value_type=value_type,
                config=config,
                options_message=_marshaller_options_message(
                    value_type=value_type,
                    value_options=marshaller_options,
                    object_options=object_marshaller_options,
                ),
            )
        # our value type is object!
        # may be we should print warning here?
        value_type = get_value_type_of_type(object)
    return MarshallerCtrl(target=target, marshaller=marshaller, value_type=value_type)


def register_marshaller(value_type, file_format, marshaller_cls):
    MARSHALERS.setdefault(value_type, {})[file_format] = marshaller_cls


def register_marshallers(value_type, marshaller_dict):
    marshaller_value_type = MARSHALERS.setdefault(value_type, {})
    for file_format, marshaller_cls in six.iteritems(marshaller_dict):
        marshaller_value_type[file_format] = marshaller_cls
