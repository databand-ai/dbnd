from pandas.core.util.hashing import hash_pandas_object

from dbnd._core.utils import json_utils
from dbnd._vendor import fast_hasher
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values.pandas_values import DataFrameValueType


class TestDataFrameValueType(object):
    def test_df_value_meta(self, pandas_data_frame):
        expected_data_schema = {
            "type": DataFrameValueType.type_str,
            "columns": list(pandas_data_frame.columns),
            "size": int(pandas_data_frame.size),
            "shape": pandas_data_frame.shape,
            "dtypes": {
                col: str(type_) for col, type_ in pandas_data_frame.dtypes.items()
            },
        }

        meta_conf = ValueMetaConf.enabled()
        expected_value_meta = ValueMeta(
            value_preview=DataFrameValueType().to_preview(
                pandas_data_frame, preview_size=meta_conf.get_preview_size()
            ),
            data_dimensions=pandas_data_frame.shape,
            data_schema=expected_data_schema,
            data_hash=fast_hasher.hash(
                hash_pandas_object(pandas_data_frame, index=True).values
            ),
        )

        df_value_meta = DataFrameValueType().get_value_meta(
            pandas_data_frame, meta_conf=meta_conf
        )

        assert df_value_meta.value_preview == expected_value_meta.value_preview
        assert df_value_meta.data_hash == expected_value_meta.data_hash
        assert json_utils.dumps(df_value_meta.data_schema) == json_utils.dumps(
            expected_value_meta.data_schema
        )
        assert df_value_meta.data_dimensions == expected_value_meta.data_dimensions
        assert df_value_meta == expected_value_meta
