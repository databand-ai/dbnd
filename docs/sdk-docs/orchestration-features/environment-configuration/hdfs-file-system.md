---
"title": "HDFS File System"
---
## Before You Begin
You must have the `dbnd-hdfs` plugin installed.

## To Set up an Environment for Hadoop Distributed File System

1. To set up HDFS access, in the `HdfsConfig` object or `[hdfs]` section of the **project.cfg** file, define the following properties:
* Host (`localhost` by default)
* Port (`50070` by default)
* User (`root` by default)
* Client Type (`knox` - by default, allowed values - `insecure`, `kerberos` or `knox`). 

Here is the configuration example:

```ini
[hdfs]
host = localhost
port = 50070
user = root
client_type = knox
knox_gateway=bigdata
```

2. Depending on your client type, additionally define the following properties:

``` Knox
knox_base = parameter.c(default=None)[str]
knox_secure = parameter.c(default=False)[bool]
knox_gateway = parameter.c(default=None)[str]
knox_password = parameter.c(default=None)[str]
knox_cookies = parameter.c(default=None)[str]
knox_bearer_token = parameter.c(default=None)[str]
knox_bearer_token_encode = parameter.c(default=True)[bool]
```
``` Kerberos
kerberos_mutual_authentication = parameter.c(default=KerberosMutualAuthentication.REQUIRED)[str]
kerberos_service = parameter.c(default="HTTP")[str]
kerberos_delegate = parameter.c(default=False)[bool]
kerberos_force_preemptive = parameter.c(default=False)[bool]
kerberos_principal = parameter.c(default=None)[str]
kerberos_hostname_override = parameter.c(default=None)[str]
kerberos_sanitize_mutual_error_response = parameter.c(default=True)[bool]
kerberos_send_cbt = parameter.c(default=True)[bool]
```

3. To write outputs to HDFS, specify the output configuration: 
```project.cfg
[output]
path_task = hdfs://{task_target_date}{sep}{task_name}{sep}{task_name}{task_class_version}_{task_signature}{sep}{output_name}{output_ext}
```