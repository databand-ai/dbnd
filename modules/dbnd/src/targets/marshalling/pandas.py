from __future__ import absolute_import

import logging

import pandas as pd
import six

from pandas import RangeIndex
from pandas.api.types import is_categorical_dtype
from pandas.core.generic import NDFrame

from dbnd._core.errors import friendly_error
from dbnd._core.utils.structures import combine_mappings
from targets.fs import FileSystems
from targets.marshalling.marshaller import Marshaller
from targets.target_config import FileCompressions, FileFormat
from targets.utils.performance import target_timeit


logger = logging.getLogger(__name__)

PANDAS_CACHE_KEY = "pandas.DataFrame"
PANDAS_COPY_ON_READ = True

PANDAS_COMPRESSION_LIB = {
    FileCompressions.gzip: "gzip",
    FileCompressions.bzip: "bz2",
    FileCompressions.xz: "xz",
    FileCompressions.zip: "zip",
}


def _get_compression_args(target, compression_arg_name):
    file_compression = target.config.compression
    if not compression_arg_name or not file_compression:
        return {}

    compression = PANDAS_COMPRESSION_LIB.get(file_compression)
    if not compression:
        logger.info("Can't find compression lib for %s", file_compression)

    return {compression_arg_name: compression}


def _data_frame_set_index(df, set_index):
    # our frame already has index equal to the one requested by user
    if df.index.name == set_index:
        return df

    # so we have index and it's not equal to current
    if not _is_default_index(df):
        df.reset_index()

    df = df.set_index(set_index)
    return df


def _is_default_index(df):
    if df.index is None:
        return False
    return isinstance(df.index, RangeIndex) or df.index.name is None


def _file_open_mode(target, mode="r"):
    try:
        mode = mode + "b" if target.config.is_binary else mode
    except Exception as ex:
        logger.warning("Could not calculate file open mode, returning %s. %s", mode, ex)
        pass

    return mode


class _PandasMarshaller(Marshaller):
    type = pd.DataFrame
    support_directory_read = False

    _compression_read_arg = None
    _compression_write_arg = None

    support_cache = False
    disable_default_index = False

    def __init__(self, series=False):
        self.pandas_series = series

    def support_direct_read(self, target):
        # pandas actually support s3
        # TODO: decide based on pandas version and s3fs availability
        return (
            super(_PandasMarshaller, self).support_direct_access(target)
            or target.fs_name == FileSystems.s3
        )
        # s3 is supported for read, but not in write ( "_get_handler"..)

    def support_direct_write(self, target):
        # TODO: decide based on pandas version and s3fs availability
        return self.support_direct_access(target)

    def _pd_read(self, *args, **kwargs):
        pass

    def _pd_to(self, *args, **kwargs):
        pass

    @target_timeit
    def target_to_value(
        self, target, cache=True, no_copy_on_read=False, set_index=None, **kwargs
    ):
        read_kwargs = _get_compression_args(target, self._compression_read_arg).copy()
        read_kwargs.update(kwargs)

        use_cache = cache and self.support_cache
        use_cache &= target._cache.get(PANDAS_CACHE_KEY) is not None
        use_cache &= (
            "key" not in read_kwargs
        )  # support for hdf5, we don't support non default key
        if not use_cache:
            try:
                logger.info("Loading data frame from target='%s'", target)
                if self.support_direct_read(target):
                    df = self._pd_read(target.path, **read_kwargs)
                else:
                    mode = _file_open_mode(target, "r")
                    with target.open(mode) as fp:
                        df = self._pd_read(fp, **read_kwargs)
            except Exception as ex:
                raise friendly_error.failed_to_read_pandas(ex, target)
        else:
            logger.info("Loading data frame from cache: %s", target)
            df = target._cache.get(PANDAS_CACHE_KEY)
            if no_copy_on_read:
                if set_index:
                    logger.warning(
                        "You are using no_copy_on_read, with set_index, "
                        "that will change df in cache for all usages: %s" % target
                    )
            else:
                df = df.copy()

        if set_index:
            try:
                df = _data_frame_set_index(df, set_index)
            except Exception as ex:
                raise friendly_error.failed_to_set_index(ex, df, set_index, target)
        return df

    def value_to_target(self, value, target, **kwargs):
        if target.config.format != FileFormat.hdf5 and not isinstance(value, NDFrame):
            raise friendly_error.targets.failed_to_save_value__wrong_type(
                value, target, expected_type=NDFrame
            )

        # dir
        target.mkdir_parent()

        cache = kwargs.pop("cache", False)
        to_kwargs = _get_compression_args(target, self._compression_write_arg).copy()
        to_kwargs.update(kwargs)

        # if (
        #     self.file_format == StorageFormat.hdf5
        #     and to_kwargs.get("key", "data") != "data"
        # ):
        #     # support for hdf5: we can not cache non default paritions
        #     cache = False

        if cache and self.support_cache:
            if PANDAS_CACHE_KEY in target._cache is not None:
                logger.warning(
                    "Writing Pandas Data Frame to the same target for the second time! %s",
                    target,
                )
            target._cache[PANDAS_CACHE_KEY] = value

        logger.info("Saving data frame to '%s'", target)

        # we want to remove index from the formats that can't deserialize it
        # so all csv and excel should not have index saved by default
        if self.disable_default_index and _is_default_index(value):
            to_kwargs.setdefault("index", False)

        try:
            if self.support_direct_write(target):
                return self._pd_to(value, target.path, **to_kwargs)
            else:
                mode = _file_open_mode(target, "w")
                with target.open(mode) as fp:
                    return self._pd_to(value, fp, **to_kwargs)
        except Exception as ex:
            raise friendly_error.failed_to_write_pandas(ex, target)


class DataFrameToCsv(_PandasMarshaller):
    file_format = FileFormat.csv

    disable_default_index = True

    _compression_write_arg = "compression"
    _compression_read_arg = "compression"

    def _pd_read(self, *args, **kwargs):
        if self.pandas_series:
            kwargs.setdefault("squeeze", True)
            kwargs.setdefault("header", None)
        return pd.read_csv(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        return value.to_csv(*args, **kwargs)


class DataFrameToJson(_PandasMarshaller):
    file_format = FileFormat.json

    _compression_write_arg = "compression"
    _compression_read_arg = "compression"

    def _pd_read(self, *args, **kwargs):
        return pd.read_json(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        return value.to_json(*args, **kwargs)


class DataFrameToHtml(_PandasMarshaller):
    file_format = FileFormat.json

    _compression_write_arg = "compression"
    _compression_read_arg = "compression"

    def _pd_read(self, *args, **kwargs):
        return pd.read_html(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        return value.to_html(*args, **kwargs)


class DataFrameToTable(DataFrameToCsv):
    file_format = FileFormat.table


class DataFrameToPickle(_PandasMarshaller):
    file_format = FileFormat.pickle
    support_cache = True
    support_index_save = True
    _compression_write_arg = "compression"

    def _pd_read(self, *args, **kwargs):
        return pd.read_pickle(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        value.to_pickle(*args, **kwargs)


class DataFrameToParquet(_PandasMarshaller):
    file_format = FileFormat.parquet
    support_cache = True
    support_index_save = True
    support_directory_direct_read = True
    support_directory_direct_write = False

    _compression_write_arg = "compression"

    def _pd_read(self, *args, **kwargs):
        return pd.read_parquet(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        value.to_parquet(*args, **kwargs)

    def support_direct_write(self, target):
        # TODO: decide based on pandas version and s3fs availability
        return (
            super(DataFrameToParquet, self).support_direct_write(target)
            or target.fs_name == FileSystems.s3
        )


class DataFrameToFeather(_PandasMarshaller):
    support_cache = True
    support_index_save = False
    file_format = FileFormat.feather

    def _pd_read(self, *args, **kwargs):
        return pd.read_feather(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        value.to_feather(*args, **kwargs)


def _get_supported_hd5_storage_format(value):
    if (
        isinstance(value, pd.DataFrame)
        and value.select_dtypes(include=["category"]).empty
    ):
        return "fixed"

    if isinstance(value, pd.Series) and not is_categorical_dtype(value.dtypes):
        return "fixed"

    logger.warning(
        "Storage format is set to 'table' as you data contains categorical attributes. This may affect performance."
    )
    return "table"


class DataFrameToHdf5(_PandasMarshaller):
    file_format = FileFormat.hdf5
    support_cache = True
    support_index_save = True

    _compression_write_arg = "complib"

    def _pd_read(self, *args, **kwargs):
        return pd.read_hdf(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        kwargs = combine_mappings(
            {
                "key": "data",
                "format": _get_supported_hd5_storage_format(value),
                "mode": "w",
                "complib": "zlib",
            },
            kwargs,
        )
        value.to_hdf(*args, **kwargs)


class DataFrameToHdf5Table(DataFrameToHdf5):
    def _pd_to(self, value, *args, **kwargs):
        kwargs.setdefault("format", "table")
        return super(DataFrameToHdf5Table, self)._pd_to(value, *args, **kwargs)


class DataFrameDictToHdf5(_PandasMarshaller):
    file_format = FileFormat.hdf5
    support_cache = True
    support_index_save = True

    _compression_write_arg = "complib"

    def _pd_read(self, path, *args, **kwargs):
        with pd.HDFStore(path, mode="r") as store:
            return {k: store.get(k) for k in store.keys()}

    def _pd_to(self, data, file_or_path, *args, **kwargs):
        complib = kwargs.get("complib", "zlib")

        with pd.HDFStore(file_or_path, mode="w", complib=complib) as store:
            for key, partition in six.iteritems(data):
                format = kwargs.get(
                    "format", _get_supported_hd5_storage_format(partition)
                )
                store.put(key, partition, format=format, **kwargs)


class DataFrameToTsv(DataFrameToCsv):
    def _pd_read(self, path, *args, **kwargs):
        kwargs.setdefault("delimiter", "\t")
        kwargs.setdefault("error_bad_lines", False)
        return super(DataFrameToTsv, self)._pd_read(path, *args, **kwargs)

    def _pd_to(self, path, *args, **kwargs):
        kwargs.setdefault("sep", "\t")
        return super(DataFrameToTsv, self)._pd_to(path, *args, **kwargs)


class DataFrameToExcel(_PandasMarshaller):
    file_format = FileFormat.excel

    def _pd_read(self, *args, **kwargs):
        return pd.read_excel(*args, **kwargs)

    def _pd_to(self, value, *args, **kwargs):
        return value.to_excel(*args, **kwargs)
