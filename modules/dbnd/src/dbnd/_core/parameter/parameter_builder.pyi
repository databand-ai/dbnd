from typing import Any, Callable, List, Type, TypeVar, Union, overload

import numpy
import pandas as pd

from dbnd._core.configuration.config_path import ConfigPath
from dbnd._core.constants import OutputMode
from dbnd._core.parameter.constants import ParameterScope
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    _ParameterKind,
)
from dbnd._core.parameter.validators import Validator
from targets import DataTarget, Target
from targets.target_config import FileFormat, TargetConfig
from targets.types import DataList
from targets.values import ValueType

_T = TypeVar("_T")
_C = TypeVar("_C")

class ParameterFactory(object):
    parameter: ParameterDefinition
    def __init__(self, parameter: ParameterDefinition = None): ...
    @overload
    def value(
        self,
        default: _T = ...,
        description: str = ...,
        significant: bool = ...,
        name: str = ...,
        target_config: TargetConfig = ...,
        config_path: ConfigPath = ...,
        value_type: ValueType = ...,
        hidden: bool = ...,
        system: bool = ...,
        scope: ParameterScope = ...,
        from_task_env_config: bool = ...,
        kind: _ParameterKind = ...,
        output_name: str = ...,
        output_ext: str = ...,
        output_mode: OutputMode = ...,
        output_factory: Callable[[Any, ParameterDefinition], Any] = ...,
        flag: bool = ...,
        validator: Validator = ...,
        choices: List = ...,
        parameter_cls: Type = ...,
        load_on_build: bool = ...,
        empty_default: bool = ...,
        log_histograms: bool = ...,
        log_size: bool = ...,
        log_stats: bool = ...,
        log_schema: bool = ...,
        log_preview: bool = ...,
        log_preview_size: int = ...,
    ) -> _T: ...
    @overload
    def value(
        self,
        default: None = ...,
        significant: bool = ...,
        description: str = ...,
        scope: ParameterScope = ...,
    ) -> ParameterFactory: ...
    def __call__(
        self,
        default: _T = ...,
        description: str = ...,
        significant: bool = ...,
        name: str = ...,
        target_config: TargetConfig = ...,
        config_path: ConfigPath = ...,
        value_type: ValueType = ...,
        hidden: bool = ...,
        system: bool = ...,
        scope: ParameterScope = ...,
        from_task_env_config: bool = ...,
        kind: _ParameterKind = ...,
        output_name: str = ...,
        output_ext: str = ...,
        output_mode: OutputMode = ...,
        output_factory: Callable[[Any, ParameterDefinition], Any] = ...,
        flag: bool = ...,
        validator: Validator = ...,
        choices: List = ...,
        parameter_cls: Type = ...,
        load_on_build: bool = ...,
        empty_default: bool = ...,
        log_histograms: bool = ...,
        log_size: bool = ...,
        log_stats: bool = ...,
        log_schema: bool = ...,
        log_preview: bool = ...,
        log_preview_size: int = ...,
    ) -> ParameterFactory: ...
    def __getitem__(self, type_: Type[_C]) -> _C: ...
    def modify(self, **kwargs: Any) -> ParameterFactory: ...
    @property
    def none(self) -> ParameterFactory: ...
    ########
    # build parameter
    @property
    def _p(self) -> ParameterDefinition: ...
    def build_parameter(self, str) -> ParameterDefinition: ...
    def _build_parameter(self, str) -> ParameterDefinition: ...
    def _build_value_type(self, str) -> ValueType: ...
    ########
    # formats
    def target_config(self, target_config: TargetConfig) -> ParameterFactory: ...
    @property
    def _target_config(self) -> TargetConfig: ...
    @property
    def no_ext(self) -> DataTarget: ...
    @property
    def gz(self) -> ParameterFactory: ...
    @property
    def gzip(self) -> ParameterFactory: ...
    @property
    def parquet(self) -> ParameterFactory: ...
    @property
    def json(self) -> ParameterFactory: ...
    @property
    def hdf5(self) -> ParameterFactory: ...
    @property
    def txt(self) -> ParameterFactory: ...
    @property
    def csv(self) -> ParameterFactory: ...
    @property
    def tsv(self) -> ParameterFactory: ...
    @property
    def pickle(self) -> ParameterFactory: ...
    @property
    def feather(self) -> ParameterFactory: ...
    @property
    def excel(self) -> ParameterFactory: ...
    @property
    def file_numpy(self) -> ParameterFactory: ...
    @property
    def require_local_access(self) -> ParameterFactory: ...
    @property
    def no_extension(self) -> ParameterFactory: ...
    @property
    def no_env_interpolation(self) -> ParameterFactory: ...
    @property
    def disable_jinja_templating(self) -> ParameterFactory: ...
    def load_options(
        self, file_format: FileFormat, **options: Any
    ) -> ParameterFactory: ...
    def save_options(
        self, file_format: FileFormat, **options: Any
    ) -> ParameterFactory: ...
    @overload
    def custom_target(self, target_factory: Type[_C], folder=False) -> _C: ...
    @overload
    def custom_target(self, target_factory: Callable, folder=False) -> Target: ...
    @overload
    def target_factory(self, target_factory: Callable, folder=False) -> Target: ...
    @property
    def folder(self) -> ParameterFactory: ...
    @property
    def folder_data(self) -> ParameterFactory: ...
    def with_flag(self, flag: Union[bool, str, None] = True) -> ParameterFactory: ...
    @property
    def prod_immutable(self) -> ParameterFactory: ...
    # kind changes
    @property
    def output(self) -> ParameterFactory: ...
    def cast(self, type_: Type[_C]) -> _C: ...
    def sub_type(self, sub_type) -> ParameterFactory: ...
    def type(self, type_) -> ParameterFactory: ...
    def parameter_cls(self, parameter_cls: ParameterDefinition) -> ParameterFactory: ...
    @property
    def data(self) -> DataTarget: ...
    def help(self, help_message: str) -> ParameterFactory: ...
    @property
    def non_significant(self) -> ParameterFactory: ...
    def config(self, section: str, name: str) -> ParameterFactory: ...
    def description(self, help_message: str) -> ParameterFactory: ...
    def default(self, value: _T) -> _T: ...
    def enum(self, enum: _T) -> _T: ...
    def choices(self, choices) -> ParameterFactory: ...
    def numerical(
        self, min_value: Union[int, float], max_value: Union[int, float]
    ) -> ParameterFactory: ...
    def custom_datetime(self, interval=1, start=None) -> ParameterFactory: ...
    def nested_config(self, config_cls: _C) -> _C: ...
    @property
    def c(self) -> ParameterFactory: ...
    @property
    def system(self) -> ParameterFactory: ...
    @property
    def target(self) -> DataTarget: ...
    @property
    def path_str(self) -> str: ...
    @property
    def pandas_dataframe(self) -> pd.DataFrame: ...
    @property
    def data_list_str(self) -> DataList[str]: ...
    @property
    def data_list(self) -> DataList: ...
    @property
    def numpy_array(self) -> numpy.ndarray: ...
    @property
    def overwrite(self) -> ParameterFactory: ...

@overload
def value(
    default: None = ...,
    significant: bool = ...,
    description: str = ...,
    scope: ParameterScope = ...,
) -> ParameterFactory: ...
@overload
def value(
    default: _T,
    significant: bool = ...,
    description: str = ...,
    scope: ParameterScope = ...,
) -> _T: ...

PARAMETER_FACTORY: ParameterFactory
data: ParameterFactory
output: ParameterFactory
parameter: ParameterFactory

def get_parameter_cls(value_type: ValueType) -> ParameterDefinition: ...
def build_parameter(
    parameter: Union[ParameterFactory, ParameterDefinition], context: str
) -> ParameterDefinition: ...
def parameter_config(p: ParameterDefinition, key: str) -> ParameterFactory: ...
