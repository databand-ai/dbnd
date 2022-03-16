---
"title": "Overriding Values"
---
## Before you begin
See the [Configuration Layers](doc:configuration-layers) document to understand what configuration layers are supported in DBND and how they work together.

In the following example, two tasks are defined: 
* `train_model_pipeline` is a pipeline that wraps `calculate_alpha`. 
* `calculate_alpha` has the default parameter value set to 0.5, and the constructor configuration sets `alpha` at 0.1.
```python
@task
def calculate_alpha(alpha=0.5):
    return alpha

@pipeline
def train_model_pipeline():
    calculate_alpha(0.1)
```


To circumvent all previous attributed values defined for `alpha` and define a new value, run the `--override` command:
```bash
dbnd run train_model_pipeline --task-version now --override calculate_alpha.alpha=0.2
```
When you execute the command, `alpha` is going to be equal to 0.2.

> **Note**
>
> The `--override` command cannot be overridden. Hence, it should be used only when it is absolutely necessary. In most cases, other layers (such as `--set`) are more suitable.

Let's use the same example we used for `--override`  with `--set`. This time our CLI will look like this:
```bash
dbnd run train_model_pipeline --task-version now --set calculate_alpha.alpha=0.2
```
In our command, we only changed `--override` to `--set`. But this time the result is different - and the value we get at execution is `0.1`.

This is because values given in task constructors supersede `--set` values.