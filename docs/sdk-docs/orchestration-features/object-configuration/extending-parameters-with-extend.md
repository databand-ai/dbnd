---
"title": "Extending Values"
---
You can extend any parameter in your environment with the help of `--extend` wrapper. It works similarly to `set` and also replaces the deprecated `spark_conf_extension`. This feature emerged as a replacement for the deprecated `spark_conf_extension` and it can be very useful for testing, to override the existing configuration and try something else and make the text environment more flexible.

In CLI, there is `override from config`, which is equivalent to `--extend`, but `--extend` is cross-layer so it’s applicable when you need to extend the previous layers.

For example `--extend` allows you to create more (static) Kubernetes labels per task (defined as list at `KuberneticeEngineConfig`). You can use `--extend` to extend a configuration instead of overriding it.

You can extend multiple times. In this case, `--extend` works like `update in a dict`.

## Using extend

Lets assume we have our KubernetesConfig with those labels in our cfg.
```
[kubernetes]
...
labels = {"team": "databand_team"}
```

and we want to extend those labels for my tasks
There are two ways to use extend:

1. Like a parameter:

```python
from dbnd import task, extend

@task(task_config={KubernetesConfig.labels: extend({"owner": "Joey"} )})
def calculate_alpha():
    pass
```

2. From the command line:
```bash
dbnd run calculate_alpha --extend kubernetes.labels={"owner":"Joey"}
```

And now the kubernetes labels will include both the "owner" and "team" labels.

## Using --extend instead of override

You can use `--extend` the way you use `override` - to wrap up the config dictionary with `--extend`. You can import `--extend` directly from DBND and wrap up the dictionary. As a result, you can extend the labels (e.g., task_name in the example code below) or any resources (e.g., limits).

```python
from dbnd import task, extend

@task(task_config={KubernetesConfig.labels: extend({"owner": "Joey"})})
def calculate_alpha():
    pass
```

If you need to override something in the dictionary, DBND supports extending dictionaries and lists. Extending a dictionary works like appending/updating the dict so if you extend a dictionary with a key `a` and you extend it with another dict with the key `a`, then the last one will override the first one.

## Using --extend from the CLI

You can also use `--extend` from the CLI and extend any configuration. This will work similarly to `set` and uses the same syntax, but instead of `set`, you need to use `--extend`. It’s possible to extend anything (e.g., trackers). If something cannot be extended, DBND will raise an error.

For example:
```bash
dbnd run prepare_data --extend spark.conf={"spark.driver.memory": "4G"}
```

It’s not recommended to extend strings because it can lead to issues.

> Keep in mind
> If you override something in a lower layer and then have `extend` in a new layer, for `extend` to work correctly, you’ll need to override both the highest priority layer and `extend`, which is a difficult way to work.