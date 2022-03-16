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

## Livy Spark Configurations

| Parameter | Description |
|---|---|
| url | Livy connection url (for example: http://livy:8998) |
| auth | Livy auth , support list are None, Kerberos, Basic_Access (Default: "None") |
| user | Livy auth , user (Default: "") |
| password | Livy auth , password (Default: "") |
| ignore_ssl_errors | Ignore ssl error (Default: False) |
| job_submitted_hook | User code to run after livy batch submit (a reference to a function)  expected interface:  (LivySparkCtrl, Dict[str, Any]) -> None (Default: None). example: `my_package.job_submitted_hook` |
| job_status_hook | User code to run at each livy batch status update (a reference to a function) expected interface:(LivySparkCtrl, Dict[str, Any]) -> None" |
| retry_on_status_error | Retries http requests if status code is not accepted (Default: 0) |

### Q: How can I debug HTTP requests related to the Livy server?
A: You can always run specific modules at debug state: 
`dbnd run ….. --set log.at_debug=module.you.want.to.debug`
In our case, you want to log both livy_batch and reliable_http_client:
`dbnd run ….. --set log.at_debug=dbnd._core.utils.http.reliable_http_client --set log.at_debug=dbnd_spark.livy.livy_batch`