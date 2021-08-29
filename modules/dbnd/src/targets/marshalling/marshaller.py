import abc
import typing


if typing.TYPE_CHECKING:
    pass


class Marshaller(object):
    type = None
    file_format = None

    # if true on folder read we use load funcition
    support_directory_direct_read = False
    support_multi_target_direct_read = False
    support_directory_direct_write = False

    clears_types_to_str = False

    @abc.abstractmethod
    def target_to_value(self, target, **kwargs):
        pass

    @abc.abstractmethod
    def value_to_target(self, value, target, **kwargs):
        pass

    def support_direct_access(self, target):
        return target.fs.support_direct_access
