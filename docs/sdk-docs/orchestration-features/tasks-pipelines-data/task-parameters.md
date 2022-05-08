---
"title": "Parameters"
---
Most of the time, you can control your parameters' behavior by using standard Python syntax.
Parameter type and a default value can be defined like in the following example:

```python
from dbnd import task
from pandas import DataFrame

@task
def prepare_data(data: DataFrame=None) -> DataFrame:
      return data
```
Occasionally, more info needs to be provided to the parameter definition.
So DBND introduces the `ParamaterFactory` concept to control it.

Every decorated task parameter can be controlled via parameter definition at function:
 ```python
from dbnd import task, parameter
from pandas import DataFrame

@task
def prepare_data(data = parameter.default(None)[DataFrame] ) -> DataFrame:
      return data
```

 If you want to keep your function code in touch, you can do that at the decorator.
 ```python
from dbnd import task, parameter
from pandas import DataFrame

@task(data = parameter.default(None)[DataFrame])
def prepare_data(data) -> DataFrame:
      return data
```

### Default Value
You can use `value` factory. This defines the default value and also infers parameter type for you, i.e.:

```python
from dbnd import task, parameter
from pandas import DataFrame

@task(alpha = parameter.value(0.5))
def calculate_alpha(alpha) -> DataFrame:
      return alpha
```

>👍 Parameter value resolution
>
> For parameter value resolution, see [DBND Object Configuration](doc:object-configuration).

## Parameter Types

In the examples above, the *type* of the parameter is determined by using `dbnd.parameter`.

Python is not a statically typed language and you don't have to specify the types
of any of your parameters. You can simply use the base class `dbnd.parameter[object]`.

The reason you would use an explicit typing like `parameter[datetime.date]` is that DBND needs to know its type for the command line interaction. That's how it knows how to convert a string provided via the command line to the corresponding type (i.e. `datetime.date` instead of a string).
DBND tasks support a variety of parameters.

Python simple types are supported:

```python
from dbnd import task

@task
def prepare_data(v_int: int, v_bool: bool, v_str: str) -> str:
    pass
```

`datetimes` types and many other basic types are also supported:

```python
from dbnd import task
from datetime import datetime, date, timedelta

@task
def prepare_data(
    v_datetime: datetime, v_date: date, v_period: timedelta
) -> str:
    pass
```

A list of various types can be provided:
```python
from dbnd import task
from datetime import date
from typing import List

@task
def prepare_data(
    v_list: List,
    v_list_obj: List[object],
    v_list_str: List[str],
    v_list_int: List[int],
    v_list_date: List[date],
) -> str:
     pass
```

Paths can be provided using strings, pathlib Path object, or DBND target types:
```python
from dbnd import task
from targets.types import PathStr, Path
from targets import Target

@task
def prepare_data(pathlib_path: Path, str_as_path: PathStr, target_path: Target) -> str:
    assert isinstance(str_as_path, str)
    pass
```
If the path is input from another task, DBND will not resolve it into an "in-memory" type, but will keep the Path as an input to function.

## Insignificant Parameters

If a parameter is created with `significant=False`, it is ignored as far as the task signature is concerned. Tasks created that differ only in insignificant parameters have the same signature and therefore are the same instance:

```python
from dbnd import parameter, task

@task
def calculate_alpha(alpha = parameter[int], debug_level=parameter(significant=False).value(0)[int]):
    return alpha
```
In this case, changes of `debug_level` will not affect the task's signature.

## Parameter Scope

Usually, you have to bypass parameters explicitly into the task constructor. If you want to "inherit" parameter from the parent scope of the task, you can use the `scope=ParameterScope.children` modifier.
```python
from dbnd import parameter, ParameterScope

parameter_with_value_from_parent = parameter(scope=ParameterScope.children)
```