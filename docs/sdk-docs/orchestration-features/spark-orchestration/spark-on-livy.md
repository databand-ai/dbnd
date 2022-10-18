---
"title": "Spark on Livy"
---
## Configuring Apache Livy

Apache Livy can be used by Cloudera Hadoop Distribution, Amazon EMR or custom-made Spark cluster.

1. Set `spark_engine` to `livy` to use Apache Livy as a way to submit spark jobs.

``` ini
[spark]
spark_engine = livy
```

2.Set Apache Livy Url in DBND config

``` ini
[livy]
url = http://<livy_server_url>:8998
```

## `[livy]` Configuration Section Parameter Reference
- `root` - Data outputs location override
- `disable_task_band` - Disable task_band file creation
- `url` - Determine livy's connection url, e.g. `http://livy:8998`
- `auth` - Set livy auth, e.g. None, Kerberos, or Basic_Access
- `user` - Set livy auth user.
- `password` - Set livy auth password.
- `ignore_ssl_errors` - Enable ignoring ssl errors.
- `job_submitted_hook` - Set the user code to be run after livy batch submit. This is a reference to a function. The expected interface is `(LivySparkCtrl, Dict[str, Any]) -> None`
- `job_status_hook` - Set the user code to be run at each livy batch status update. This is a reference to a function. The expected interface is `(LivySparkCtrl, Dict[str, Any]) -> None`
- `retry_on_status_error` - Set the number of retries for http requests if the status code is not accepted.
- `retry_on_status_error_delay` - Determien the amount of time, in seconds, in between retries for http requests.


### Q: How can I debug HTTP requests related to the Livy server?
A: You can always run specific modules at debug state:
`dbnd run ….. --set log.at_debug=module.you.want.to.debug`
In our case, you want to log both livy_batch and reliable_http_client:
`dbnd run ….. --set log.at_debug=dbnd._core.utils.http.reliable_http_client --set log.at_debug=dbnd_spark.livy.livy_batch`
