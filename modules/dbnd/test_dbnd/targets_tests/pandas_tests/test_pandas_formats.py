# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging

import pytest

from pandas.util.testing import assert_frame_equal

from targets import target
from targets.extras.pandas_ctrl import PandasMarshallingCtrl
from targets.target_config import FileFormat, file


logger = logging.getLogger(__name__)

TEST_EXTENSIONS = [
    file.csv,
    file.csv.gzip,
    file.json,
    file.json.gzip,
    file.feather,
    file.hdf5,
    file.parquet,
    file.pickle,
]

_FORMATS_WITH_INDEX_SUPPORT = [file.hdf5, file.parquet, file.pickle]
_FORMATS_WITHOUT_INDEX_SUPPORT = [file.csv, file.with_format(FileFormat.table)]

# USE for read/write functions that doesn't match extension
PANDAS_READ_WRITE_CUSTOM = {
    file.hdf5: (PandasMarshallingCtrl.read_hdf, PandasMarshallingCtrl.to_hdf)
    #     FF.json: (PandasTargetCtrl.read_json, PandasTargetCtrl.to_json),
    #     FF.parquet: (PandasTargetCtrl.read_parquet, PandasTargetCtrl.to_csv)
}


def _get_read_write_functions(target_config):
    if target_config in PANDAS_READ_WRITE_CUSTOM:
        read_f, to_f = PANDAS_READ_WRITE_CUSTOM[target_config]
    else:
        read_f = getattr(PandasMarshallingCtrl, "read_%s" % target_config.format)
        to_f = getattr(PandasMarshallingCtrl, "to_%s" % target_config.format)

    def _read_f(t, *args, **kwargs):
        return read_f(t.as_pandas, *args, **kwargs)

    def _to_f(t, *args, **kwargs):
        return to_f(t.as_pandas, *args, **kwargs)

    return _read_f, _to_f


class TestPandasFormatsTarget(object):
    @pytest.mark.parametrize("target_config", TEST_EXTENSIONS)
    def test_auto_extension(self, target_config):
        t = self.get_target(target_config)

        # let check auto extension discovery
        assert t.config == target_config
        assert t.config.compression == target_config.compression

    @pytest.mark.parametrize("target_config", _FORMATS_WITH_INDEX_SUPPORT)
    def test_types_with_schema_no_index(self, target_config, pandas_data_frame):
        self.validate(df=pandas_data_frame, target_config=target_config)

    @pytest.mark.parametrize("target_config", _FORMATS_WITH_INDEX_SUPPORT)
    def test_types_with_schema_with_index(self, target_config, pandas_data_frame_index):
        self.validate(df=pandas_data_frame_index, target_config=target_config)

    @pytest.mark.parametrize("target_config", _FORMATS_WITHOUT_INDEX_SUPPORT)
    def test_index_support_in_simple_formats(
        self, target_config, pandas_data_frame_index
    ):
        self.validate(
            df=pandas_data_frame_index,
            target_config=target_config,
            read_kwargs=dict(set_index="Names"),
        )

    def test_csv_simple(self):
        self.validate(
            self.df_simple,
            file.csv,
            read_kwargs=dict(index_col=False),
            write_kwargs=dict(index=False),
        )

    def test_csv_index(self):
        self.validate(self.df_index, file.csv, read_kwargs=dict(set_index="Names"))

    def test_hd5_categorical(self):
        self.validate(self.df_categorical, file.hdf5)

    def test_csv_gz(self):
        self.validate(
            self.df_simple,
            file.csv.gzip,
            read_kwargs=dict(index_col=False),
            write_kwargs=dict(index=False),
        )

    # def test_json(self, pandas_data_frame):
    #     self.validate(self.df_simple, FF.json)

    def validate(self, df, target_config, read_kwargs=None, write_kwargs=None):
        target_read_f, target_write_f = _get_read_write_functions(target_config)
        write_kwargs = write_kwargs or {}
        read_kwargs = read_kwargs or {}

        t = self.get_target(target_config)

        # regular write - should automatically select format
        df.to_target(t, **write_kwargs)

        # now we read with automatic read
        actual_1 = t.read_df(**read_kwargs)
        assert_frame_equal(actual_1, df)
        assert id(actual_1) != id(df)

        # use explicit function
        actual_2 = target_read_f(t, **read_kwargs)
        assert_frame_equal(actual_2, df)
        assert id(actual_2) != id(df)

        # validate  with different target
        t_2 = target(t.path)
        actual = target_read_f(t_2, **read_kwargs)
        assert_frame_equal(actual, df)

        # validate write via pandas ctrl
        t_3 = self.get_target(target_config, name="local_file_2")
        assert not t_3.exists()
        target_write_f(t_3, df, **write_kwargs)
        actual = target_read_f(t_3, **read_kwargs)
        assert_frame_equal(actual, df)

    @pytest.fixture(autouse=True)
    def set_tmp_dir(self, tmpdir, pandas_data_frame, df_categorical):
        self.tmpdir = tmpdir
        self.df_simple = pandas_data_frame
        self.df_index = pandas_data_frame.set_index("Names")
        self.df_categorical = df_categorical

    def get_target(self, config, name="local_file"):
        ext = config.get_ext()

        local_file = str(self.tmpdir.join("%s%s" % (name, ext)))
        return target(local_file)
