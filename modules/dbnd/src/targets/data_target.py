# © Copyright Databand.ai, an IBM Company 2022

import abc
import logging

from dbnd._core.errors import friendly_error
from dbnd._core.utils.basics.nothing import NOTHING, is_not_defined
from targets import Target
from targets.caching import TARGET_CACHE, TargetCacheKey
from targets.errors import NotADirectory
from targets.extras.file_ctrl import ObjectMarshallingCtrl
from targets.marshalling import get_marshaller_ctrl
from targets.utils.performance import target_timeit_log


logger = logging.getLogger(__name__)


class DataTarget(Target):
    """
    Target with Data
    """

    target_no_traverse = True

    def __init__(self, properties=None, source=None):
        """
        Initializes a DataTarget instance.

        :param str path: the path associated with this DataTarget.
        """
        super(DataTarget, self).__init__(properties=properties, source=source)

        try:
            import pandas  # noqa: F401

            from targets.extras.pandas_ctrl import PandasMarshallingCtrl

            self.as_pandas = PandasMarshallingCtrl(self)
        except ImportError:
            pass

        self.as_object = ObjectMarshallingCtrl(self)

    @abc.abstractmethod
    def open(self, mode="r"):
        """
        Open the DataTarget target.

        This method returns a file-like object which can either be read from or written to depending
        on the specified mode.

        :param str mode: the mode `r` opens the DataTarget in read-only mode, whereas `w` will
                         open the DataTarget in write mode. Subclasses can implement
                         additional options.
        """

    def _dt(self):
        from targets.data_transaction import DataTransaction

        return DataTransaction(self)

    @abc.abstractmethod
    def remove(self):
        """
        Remove the resource at the path specified by this DataTarget.

        This method is implemented by using :py:attr:`fs`.
        """

    def mark_success(self):
        """
        marks current target as exists.
        :return:
        """

    def list_partitions(self):
        return [self]

    def partition(self, name=NOTHING, **kwargs):
        raise NotADirectory(
            "You can create paritions for DirTarget only. "
            "Please validate that you have .folder flag and you path ends with slash: %s"
            % str(self)
        )

    def file(self, name=NOTHING, extension=NOTHING, config=NOTHING, **kwargs):
        """
        :param config:
        :param name: file name of the partition. if not provided - "part-%04d" % ID
        :param extension: extension. if not provided -> default extension will be used
        :return: FileTarget: that represents the partition.
        """
        config = self.config if is_not_defined(config) else config
        return self.partition(
            name=name, extension=extension, config=config.as_file(), **kwargs
        )

    def folder(self, name=NOTHING, extension=NOTHING, config=NOTHING, **kwargs):
        """
        :param config:
        :param name: file name of the partition. if not provided - "part-%04d" % ID
        :param extension: extension. if not provided -> default extension will be used
        :return: DirTarget: that represents the partition.
        """

        config = self.config if is_not_defined(config) else config
        return self.partition(
            name=name, extension=extension, config=config.as_folder(), **kwargs
        )

    def save(self, value):
        return self.dump(value=value)

    def dump(self, value, value_type=None, **kwargs):
        from targets.values import (
            ObjectValueType,
            get_value_type_of_obj,
            get_value_type_of_type,
        )

        if value_type:
            value_type = get_value_type_of_type(value_type)
        else:
            value_type = get_value_type_of_obj(value, ObjectValueType())
        try:
            m = get_marshaller_ctrl(self, value_type_or_obj_type=value_type)

            with target_timeit_log(self, "marshalling"):
                m.dump(value, **kwargs)
        except Exception as ex:
            raise friendly_error.failed_to_write_task_output(
                ex, self, value_type=value_type
            )
        cache_key = TargetCacheKey(target=self, value_type=value_type)
        TARGET_CACHE[cache_key] = value

    def load(self, value_type, **kwargs):
        cache_key = TargetCacheKey(target=self, value_type=value_type)
        if cache_key in TARGET_CACHE:
            logger.info("Using cached data value for target='%s'", self)
            return TARGET_CACHE[cache_key]

        m = get_marshaller_ctrl(self, value_type)

        with target_timeit_log(self, "unmarshalling"):
            value = m.load(**kwargs)

        TARGET_CACHE[cache_key] = value

        return value

    def touch(self):
        return self.as_object.touch()

    # aliases fro the most used functions
    def readlines(self, **kwargs):
        return self.as_object.readlines(**kwargs)

    def read(self, **kwargs):
        return self.as_object.read(**kwargs)

    def read_obj(self, **kwargs):
        return self.as_object.read_obj(**kwargs)

    def read_pickle(self, **kwargs):
        return self.as_object.read_pickle(**kwargs)

    def write(self, fn, **kwargs):
        self.as_object.write(fn, **kwargs)

    def writelines(self, fn, **kwargs):
        self.as_object.writelines(fn, **kwargs)

    def write_pickle(self, obj, **kwargs):
        self.as_object.write_pickle(obj, **kwargs)

    def write_numpy_array(self, arr, **kwargs):
        self.as_object.write_numpy_array(arr, **kwargs)

    def read_df(self, **kwargs):
        return self.as_pandas.read(**kwargs)

    def read_df_partitioned(self, **kwargs):
        for t in self.as_pandas.read_partitioned(**kwargs):
            yield t

    def write_df(self, df, **kwargs):
        return self.as_pandas.to(df, **kwargs)
