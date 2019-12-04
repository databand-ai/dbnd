import six

from dbnd._core.utils import json_utils
from targets.values.pandas_values import DataFrameValueType


class TestDataFrameValueType(object):
    def test_data_dimensions(self, pandas_data_frame):
        dimensions = DataFrameValueType().get_data_dimensions(pandas_data_frame)
        assert dimensions == (5, 2)

    def test_data_shape(self, pandas_data_frame):
        expected_schema = json_utils.dumps(
            {
                "type": "DataFrame",
                "columns": list(pandas_data_frame.columns),
                "size": int(pandas_data_frame.size),
                "dtypes": {"Births": "int64", "Names": "object"},
                "shape": pandas_data_frame.shape,
            }
        )
        schema = DataFrameValueType().get_data_schema(pandas_data_frame)
        assert isinstance(schema, six.string_types)
        assert schema == expected_schema
