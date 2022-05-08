---
"title": "DBND Plugins"
---
## DBND Loading Plugins at Startup

You can extend the DBND SDK's functionality via pip-installable plugins.
Several DBND packages do exactly that, including `dbnd-aws`, `dbnd-hdfs`, `dbnd-gcp`, and more.

You can find the full list of dbnd plugins [here](doc:​​installing-dbnd).

DBND uses the `pluggy` library to load plugins during the bootstrap phase, which takes place every time you run a Pipeline. More information about `pluggy` can be found [here]( https://pluggy.readthedocs.io/en/latest/).

## Making Plugins Work with DBND

There are two ways to make DBND to use your plugins:

**Via dbnd configuration:**
You can set the `plugins` config in the `core` section to your list of modules (separated by comma).
DBND will try to import each module and load it.

For example:

```
[core]
plugins = my_plugin._plugin
```

**Via `setuptools` entrypoint:**
You can add a dbnd `entry_point` to you `setup.py`.
For example:

```python
entry_points={"dbnd": ["dbnd-aws = dbnd_aws._plugin"]},
```

You need to place your setup code in your *_plugin.py* file.

## Writing Your Plugin Setup Code:

Your package source should contain the following function:

```python
import dbnd

@dbnd.hookimpl
def dbnd_setup_plugin():
   pass
```

To change the order in which your plugin is called relatively to other plugins, you can use the `tryfirst` or `trylast` flags. This way, your plugin is called first or last, respectively.

For example:

```python
import dbnd

@dbnd.hookimpl(trylast=True)
def dbnd_setup_plugin():
    pass
```

**Useful functions to call in dbnd_setup_plugin:**
`register_config_cls` - Registers a new config
`Register_file_system` - Useful for registering a new type of file system (for example, your own implementation of S3)
`register_marshaller` - To register your own custom defined marshaler (see [creating a custom marshaller](doc:custom-marshaller))
`register_value_type` - To register a custom value type

## Additional dbnd hooks

Other than `dbnd_setup_plugin` there are other hooks that you can use together with the `@dbnd.hookimpl` decorator to influence dbnd behaviour.

`dbnd_get_commands` - Called from main cli entry point, allows to register new commands
`dbnd_task_run_context` - Uses this context manager when running task_run