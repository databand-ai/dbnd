---
"title": "Introduction to Configuration"
---
DBND configuration system provides fine-grained control over your executions.
The DBND configuration is built in layers. You can augment and change the configuration for every task by adding new configuration layers of your own.

Metadata specified in a higher configuration layer is overridden with the configuration specified in a lower layer:

![Configuration layers in DBND](https://files.readme.io/4f52ef8-Configuration_layers_in_DBND_1.png)


| Configuration layer | Description | Reference |
|---|---|---|
| The `--override` command in CLI | This command is the most "authoritative" way to change configuration - it is absolute, and cannot be overridden, hence it should be used only in suitable situations. In most cases, other layers are suitable. | [Overriding Configuration in CLI](doc:overriding-configuration-in-cli) |
| Constructor configuration layer | The constructor configuration layer draws its data from the parameters provided when instantiating the task. | [Overriding constructor default values](doc:task-configuration-defaults) |
| Task configuration (--set) | **Task configuration is used for configuring tasks that may or may not have default values. <br> **It sets a specific value for a parameter, but does not  override the task constructor. |
| Configuration files and Environment Variables | Configuration files can be used for configuring specific tasks, as well as project-wide settings. | [Configuration files overview](doc:dbnd-sdk-configuration)|
| Default values in the task definition specified in code | Default values are the last fallback for configuration changes. If no other value is configured, the default value is used. |

# Example:
```python
from dbnd import task

@task
def calculate_alpha(alpha=0.5, beta=0.1):
    return alpha

alpha_task = calculate_alpha(alpha=4)
```

## Using the constructor configuration layer

Using this layer is as simple as overriding the default values that each task defines when invoking the task constructor. In the above example, we override the positional parameter `alpha` whose default value is `0.5`. We set its value to `4`.

## Using the task configuration layer

With the CLI command:

```bash
dbnd run calculate_alpha --task-version now --set beta=0.2 --set alpha=3
```
Our `beta` value is going to be `0.2`. `--set` supersedes the task's default value.
However, `alpha` value is going to be `4`, as it has `4` coming from the constructor.

## Using configuration files for specific tasks
Let us look at the same code example again. Let's define a configuration section for the task `calculate_alpha` in `project.cfg`:

```buildoutcfg
[calculate_alpha]
alpha = 2
beta = 2
```

The value of the `beta` during execution is going to be  `2`. The configuration is given in the `project.cfg` file supersedes the task's default value, and hence overrides `beta`.
However, `alpha` is going to stay `4` as the constructor has a higher priority.
