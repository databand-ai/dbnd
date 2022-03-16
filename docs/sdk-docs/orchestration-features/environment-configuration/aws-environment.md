---
"title": "AWS Environment"
---
## Before You Begin
You must have the `dbnd-aws` plugin installed.

## To Set Up AWS Environment

1. Open the **project.cfg** file, and add `aws` to the list of environments.
2. In the `[aws]` section, configure your connection ID, and optionally provide a root bucket/folder for your data.

```ini
[core]
environments = ['local', 'aws']
...
[aws]
env_label = dev
root = s3://databand-playground/databand_project
region_name = us-east-2
```

3. To override the default `boto` credentials, used by default, run the following command:

```shell
$ dbnd airflow connections --add  \
        --conn_id aws_default \
        --conn_type S3 \
        --conn_extra "{\"aws_access_key_id\":\"_your_aws_access_key_id_\", \"aws_secret_access_key\": \"_your_aws_secret_access_key_\"}"
```