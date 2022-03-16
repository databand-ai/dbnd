---
"title": "Extending Configurations"
---
DBND Configuration system provides a framework for creating new Configs and Tasks using the configuration system

We define an `aws_prod` environment which inherits settings from an existing `aws` environment, but overrides the root to a new `production` S3 bucket, and the `spark_engine` to a production Apache Livy endpoint:

``` ini
[aws_prod]
_from = aws
env_label = prod
production = True
root = s3://databand-playground/prod
spark_engine = livy

[livy]
url = http://<ip of my cluster>:8998
```

### _type
The `_type` switch searches for an object or a section with a name matching the specified value and then creates an object of the specified type.

For example, you want to create the task `calculate_beta` that has a configuration model (parameters set) cloned from `calculate_alpha`, you can configure the task `calculate_beta` as follows:

```ini
[calculate_beta]
_type=calculate_alpha
```

The configuration will be taken from an existing task `calculate_alpha` that might look as follows:

``` 
[calculate_alpha]
  alpha=0.5
  beta=0.1
```

### _from
This switch searches for an object or a section with a name that matches the specified value and then it reuses all configurations specified in the object/section.

Now, after you have the task configuration model defined, you can set values for the task `calculate_alpha`. You might want to copy all configurations from the task `calculate_gamma`.

`calculate_gamma` is defined in the **project.cfg** as follows:

 ``` 
[calculate_alpha]
  alpha=0.4
  beta=0.2
```

The **_from ** in this example replaces configuration values from another task. So, `calculate_gamma` can be defined as follows:

``` 
  [calculate_gamma]
  _from=calculate_alp
  beta=0.25
```