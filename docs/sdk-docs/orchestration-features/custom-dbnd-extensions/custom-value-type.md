---
"title": "Custom Value Type"
---
Custom value types are used as parameters for tasks. They can be parsed from configuration files or the command line and then used in code.

## Supporting advanced DataFrames  
You can enable support for other dataframe types by implementing a  new`DataFrameValueType` and registering it with `dbnd`.

For example, you might want to log a Koalas dataframe. Koalas implements the pandas API on top of Spark.
 
In order to support Koalas, copy the code snippet below, and make sure it is running when your system is starting up:
```
import databricks.koalas as ks

from targets.values import DataFrameValueType, register_value_type


class KoalasValueType(DataFrameValueType):
    type = ks.DataFrame
    type_str = "KoalasDataFrame"

register_value_type(KoalasValueType())
```

Please notice that Koala implements Panadas API ,so base class `DataFrameValueType` can be used.


## To Define a Custom Value Type

```python
# This is a custom object we want to use as a parameter in tasks
class MyCustomObject(object):
    def __init__(self, custom):
        self.custom = custom

# This defines how MyCustomObject is parsed and serialied into strings
class _MyCustomObjectValueType(ValueType):
    type = MyCustomObject

    def parse_from_str(self, x):
        return MyCustomObject(x)

    def to_str(self, x):
        return x.custom

# This registers value type with value type registry
register_value_type(_MyCustomObjectValueType())
```





## To Use a Custom Value Type at Task definition [Task Class](doc:task-definitions-as-a-class) 

```python
class TaskWithCustomValue(PythonTask):
    custom_value = parameter.type(_MyCustomObjectValueType)
    list_of_customs = parameter.sub_type(_MyCustomObjectValueType)[List]

    report = output[str]

    def run(self):
        assert isinstance(self.custom_value, MyCustomObject)
        assert isinstance(self.list_of_customs[1], MyCustomObject)
        self.report = self.custom_value.custom + self.list_of_customs[1].custom

class TaskWithCustomValueInline(PythonTask):
    # works only after registration!
    custom_value = parameter[MyCustomObject]
    custom_value_with_default = parameter.value(MyCustomObject("1"))

    list_of_customs = parameter.sub_type(MyCustomObject)[List]
    set_of_customs = parameter.sub_type(MyCustomObject)[Set]
    dict_of_customs = parameter.sub_type(MyCustomObject)[Dict]

    report = output[str]

    def run(self):
        assert isinstance(self.custom_value, MyCustomObject)
        assert isinstance(self.custom_value_with_default, MyCustomObject)

        self.report = "_".join(
            [
                c.custom
                for c in [
                    self.custom_value,
                    self.custom_value_with_default,
                    self.list_of_customs[0],
                    self.dict_of_customs["a"],
                ]
            ]
        )
```