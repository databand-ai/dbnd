import typing

from collections import Iterable

import attr


if typing.TYPE_CHECKING:
    from typing import Callable, List, Union, Optional


def columns_converter(columns):
    # type: (Union[str,Iterable[str], Callable[[], List[str]]]) -> List
    if isinstance(columns, str):
        return columns.split(",")
    elif isinstance(columns, Iterable):
        return [str(col) for col in columns]
    elif callable(columns):
        return columns()
    return []


@attr.s
class LogDataRequest(object):
    """ Used in log_data() for with_stats, with_histograms params """

    include_columns = attr.ib(
        converter=columns_converter, factory=list
    )  # type: Union[Callable[[], List[str]], List]
    exclude_columns = attr.ib(
        converter=columns_converter, factory=list
    )  # type: Union[Callable[[], List[str]], List]
    include_all_boolean = attr.ib(default=False)  # type: bool
    include_all_numeric = attr.ib(default=False)  # type: bool
    include_all_string = attr.ib(default=False)  # type: bool

    # TODO: Make these helpers class properties
    @classmethod
    def ALL_BOOLEAN(cls):
        """Calculate statistics and histograms for all columns of boolean type(s)"""
        return cls(include_all_boolean=True)

    @classmethod
    def ALL_NUMERIC(cls):
        """Calculate statistics and histograms for all columns of Numeric type(s)"""
        return cls(include_all_numeric=True)

    @classmethod
    def ALL_STRING(cls):
        """Calculate statistics and histograms for all columns of String type(s). Usually this means all non boolean, non numeric types"""
        return cls(include_all_string=True)

    @classmethod
    def ALL(cls):
        """Calculate statistics and histograms for all columns"""
        return cls(
            include_all_boolean=True, include_all_numeric=True, include_all_string=True
        )

    @classmethod
    def NONE(cls):
        return cls()

    def __bool__(self):
        return (
            self.include_columns
            or self.include_all_numeric
            or self.include_all_boolean
            or self.include_all_string
        )

    __nonzero__ = __bool__

    @classmethod
    def from_user_param(cls, user_param):
        # type: (Optional[Union[bool, str, List[str], LogDataRequest]]) -> LogDataRequest
        """
        To allow flexibility for the user, those parameter can also be:
        - boolean to calculate or not on all the data
        - list and str to specify specific column names
        """
        if isinstance(user_param, cls):
            return user_param
        elif isinstance(user_param, bool):
            return cls.ALL() if user_param else cls.NONE()
        elif isinstance(user_param, (list, str)):
            return cls(include_columns=user_param)
