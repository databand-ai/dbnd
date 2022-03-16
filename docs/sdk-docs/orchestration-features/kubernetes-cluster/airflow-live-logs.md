---
"title": "Airflow Live Logs in Kubernetes"
---
Live logs (or ‘active logs’) is a Kubernetes-specific feature in Databand that allows you to view the Airflow live logs before the Run is finished. Standard Airflow Kubernetes Executor doesn't support log access side-car. After enabling this feature, you’ll be able to see the logs for specific tasks of the Run in the Airflow webserver.

To find the link to your Airflow task  live logs, use the button in the Databand web UI: Run > Detailed view of a Run > specific Task that is running. There you should be able to see logs from the container that is running. 

![Live logs k8 document](https://files.readme.io/dc34c7c-Live-logs-k8-document.png)

>⚠️ Note
> The live logs are only available until the Run is finished. If you want logs to persist in the Airflow UI, you need to enable Airflow remote logging.

## Enabling Live Logs for Kubernetes

To enable live logs, you will need to enable it in your [Kubernetes Engine Configuration](doc:kubernetes-engine-configuration)  via 
```
[kubernetes]
airflow_log_enabled = True
```


## Live Logs Parameter Description 
These are the parameter descriptions that you can use for enabling and configuring live logs:
 
 * `airflow_log_image` - specifies the Airflow image that will be used to add a sidecar to the Run which will expose the live logs of the Run. Airflow will reach the service running on this sidecar and take the logs from it. Put differently, this image contains a sort of API for the Airflow web server to receive and display the logs. This should be an image that has airflow installed and available via `airflow` command. Usually, the image that is used to run Databand task inside Kubernetes pod has those capabilities. So the default value is going to be aligned with your main pod image ( `container_repository`:`container_tag`). However, if for some reason, the image doesn't have that support, you take vanilla Apache Airflow image like `apache/airflow:1.10.10-python3.7`

* **trap\_exit\_ file: `kubernetes.trap_exit_file_flag`**
This file signals to Databand when the original container (the base container) finishes its Run. This is when the sidecar should finish running, too.  For example, you can use: `/tmp/pod/terminated`

* `airflow_log_folder` - specifies the location in the Airflow image (sidecar), where the logs from the original container are mounted and expose them to the Airflow UI. 
Default parameter: `/usr/local/airflow/logs`.

* `airflow_log_port`- the port Airflow live log sidecar will expose its service. This port should match the port Airflow webserver tries to access the live logs.
Default parameter: `8793`.

* `container_airflow_log_path` - the path to the Airflow logs in the Databand container.
Default parameter:`/root/airflow/logs/`.

* `host_as_ip_for_live_logs` - sets the host of the pod to be the IP address of the pod. Normally, In Kubernetes, only Services get DNS names, not pods. This IP is used for the Airflow webserver to look up the sidecar. See more [here](https://stackoverflow.com/questions/59258223/how-to-resolve-pod-hostnames-from-other-pods/59262628#59262628).
Default parameter:`True`.

```buildcfg
[kubernetes]
airflow_live_log_image=apache/airflow:1.10.10-python3.7
trap_exit_file_flag=/tmp/pod/terminated
```

### Troubleshooting

In case you have a problem with seeing Live Logs at Airflow, please run `dbnd` with extra switch `--set log.debug_log_config=True`. That will enable debug mode for our logging system, so the reason for the failure might be found.

Make sure you have a correct value of `[airflow]webserver_url` in your databand configuration. This will be used as a base URL for the generated Log link