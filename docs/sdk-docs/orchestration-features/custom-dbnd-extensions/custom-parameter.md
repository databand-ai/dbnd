---
"title": "Custom Parameter"
---
Implement and register FeatureStore as custom DataValueType.

```python
import attr
from targets.values.builtins_values import DataValueType
from pandas import DataFrame
from dbnd import parameter
from dbnd._core.parameter import register_custom_parameter

@attr.s
class FeatureStore(object):
    features = attr.ib()  # type: DataFrame
    targets = attr.ib()  # type: DataFrame


class MyFeatureStoreValueType(DataValueType):
    type = FeatureStore

    def load_from_target(self, target, **kwargs):
        features = target.partition("features").load(DataFrame)
        targets = target.partition("targets").load(DataFrame)
        return FeatureStore(features=features, targets=targets)

    def save_to_target(self, target, value, **kwargs):
        target.partition("features").save(value.features)
        target.partition("targets").save(value.targets)


FeatureStoreParameter = register_custom_parameter(
    value_type=MyFeatureStoreValueType,
    parameter=parameter.type(MyFeatureStoreValueType).folder.hdf5,
)
```

Now, we can use ```FeatureStore``` within a pipeline:
Decorators:

```python
from dbnd import task, pipeline
from pandas import DataFrame

@task(result=FeatureStoreParameter.output)
def create_feature_store(ratio=1):
    features = DataFrame(data=[[ratio, 2], [2, 3]], columns=["Names", "Births"])
    targets = DataFrame(data=[[1, 22], [2, 33]], columns=["Names", "Class"])
    return FeatureStore(features=features, targets=targets)


@task
def calculate_advance_features(feature_store):
    # type: (FeatureStore)-> DataFrame
    return feature_store.features  # simple implementation


@task
def report_features(feature_store):
    # type: (FeatureStore)-> str
    logger.warning(feature_store)
    assert (2, 2) == feature_store.features.shape
    return "OK"


@pipeline(result=("advance_features", "validation"))
def calculate_features(ratio):
    store = create_feature_store(ratio)
    advance_features = calculate_advance_features(store)
    store_validation = report_features(feature_store=store)

    return advance_features, store_validation

```

Classes:

```python
from dbnd import PythonTask, output, log_dataframe, pipeline
from pandas import DataFrame


class CreateFeatureStoreViaClass(PythonTask):
    store = FeatureStoreParameter.output

    def run(self):
        self.store = create_feature_store()


class CalculateAdvancedFeatures(PythonTask):
    store = FeatureStoreParameter[FeatureStore]
    advanced_features = output[DataFrame]

    def run(self):
        log_dataframe("features", self.store.features)
        self.advanced_features = self.store.features


@pipeline
def calculate_features_via_classes(ratio):
    store = CreateFeatureStoreViaClass().store
    return CalculateAdvancedFeatures(store=store).advanced_features
```