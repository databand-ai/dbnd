---
"title": "Kubernetes Engine Configuration"
---
## Configuring Kubernetes Engine Guide

To direct DBND to interact with your cluster, you need to update the `databand-system.cfg` configuration file with the Kubernetes Cluster connection details.

**Step 1.** In the `environments` configuration, add a new environment for the Kubernetes cluster. For example, `kubernetes_cluster_env` - you can use an existing one, but in this example, let's assume its name is 'kubernetes_cluster_env':

```ini
[core]
environments = ['local', 'local_minikube', 'kubernetes_cluster_env']
```

**Step 2.** In the configuration file, add the environments' specification for your new or existing environment (`kubernetes_cluster_env`):

```ini
    [kubernetes_cluster_env]
    _from = local
    remote_engine = your_kubernetes_engine
```

> Parameter `_from` means "from where to draw previous definitions". In this example, the `[local]` section is used (see [Extending Configurations](doc:extending-configurations)).
>
> The `remote_engine` setting defines what engine is going to run your remote tasks (submitted tasks).

**Step 3.** In the configuration file, add the engine configuration.
The following example describes the engine configuration:

```ini
    [your_kubernetes_engine]
    _type = kubernetes

    container_repository = databand_examples
    container_tag =

    docker_build = True
    docker_build_push = True

    cluster_context = databand_context
    namespace = databand
    service_account_name = databand

    debug = False
    secrets = [
       { "type":"env", "target": "AIRFLOW__CORE__SQL_ALCHEMY_CONN", "secret" : "databand-secret", "key": "airflow_sql_alchemy_conn"},
       { "type":"env", "target": "AIRFLOW__CORE__FERNET_KEY", "secret" : "databand-secret", "key": "airflow_fernet_key"},
       { "type":"env", "target": "DBND__CORE__DATABAND_URL", "secret" : "databand-secret", "key": "databand_url"}
       ]

    pod_error_cfg_source_dict = {
                                "255": {"retry_count": 3, "retry_delay": "3m"},
                                "err_image_pull": {"retry_count": 0, "retry_delay": "3m"},
                                "err_pod_deleted": {"retry_count": 3, "retry_delay": "3s"},
                                }
```

## `[kubernetes]` Configuration Section Parameter Reference
- `require_submit` - Should the task engine be forced to submit tasks
- `dbnd_local_root` - Local dbnd home directory at the engine environment
- `dbnd_executable` - 'dbnd' executable path at engine environment
- `container_repository` - Where is the Docker image repository to pull the pod images from? If you are running user code, this is where you need to supply your repository and tag settings.
- `container_tag` - If defined, Docker will not be built and the specified tag will be used.
- `container_tag_gpu` - Docker container tag for GPU tasks
- `docker_build_tag_base` - Auto build docker container tag
- `docker_build_tag` - Docker build tag for the docker image dbnd will build
- `docker_build` - Should the Kubernetes executor build the Docker image on the fly? Useful if you want a different image every time. If container_repository is unset it will be taken (along with the tag) from the docker build settings
- `docker_build_push` - Should the built Docker image be pushed to the repository? Useful for specific cases.
- `cluster_context` - The Kubernetes context; you can check which context you are on by using `kubectl config get-contexts`.
- `config_file` - Custom Kubernetes config file.
- `in_cluster` - Defines what Kubernetes configuration is used for the Kube client.Use `false` to enforce using local credentials, or `true` to enforce the `in_cluster` mode.Default: `None` (Databand will automatically decide what mode to use).
- `image_pull_policy` - Kubernetes image_pull_policy flag
- `image_pull_secrets` - The secret with the connection information for the `container_repository`.
- `keep_finished_pods` - Don't delete pods on completion
- `keep_failed_pods` - Don't delete failed pods. You can use it if you need to debug the system.
- `namespace` - The namespace in which Databand is installed inside the cluster (`databand` in this case).
- `secrets` - User secrets to be added to every created pod
- `system_secrets` - System secrets (used by Databand Framework)
- `env_vars` - Assign environment variables to the pod.
- `node_selectors` - Assign `nodeSelector` to the pods (see [Assigning Pods to Nodes](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/))
- `annotations` - Assign annotations to the pod (see [Annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/))
- `service_account_name` - You need permissions to create pods for tasks, namely - you need to have a `service_account` with the correct permissions.
- `affinity` - Assign `affinity` to the pods (see [Assigning Pods to Nodes](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/))
- `tolerations` - Assign tolerations to the pod (see [Taints and Tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/))
- `labels` - Set a list of pods' labels (see [Labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/))
- `requests` - Setting the requests for the pod can be achieved by setting this. You can provide a standard Kubernetes Dict, however, you can also use explicit keys like `request_memory` or `request_cpu`
- `limits` - Setting the limits for the pod can be achieved by setting this. You can provide a standard Kubernetes Dict, however, you can also use explicit keys like `limit_memory` or `limit_cpu`
- `pod_error_cfg_source_dict` - Allows flexibility of sending retry on pods that have failed with specific exit codes. You can provide "PROCESS EXIT CODE" as a key (for example, `137`) or Kubernetes error string.
- `pod_default_retry_delay` - The default amount of time to wait between retries of pods
- `submit_termination_grace_period` - timedelta to let the submitted pod enter a final state
- `show_pod_log` - When using this engine as the task_engine, run tasks sequentially and stream their logs
- `debug` - When true, displays all pod requests sent to Kubernetes and more useful debugging data.
- `debug_with_command` - Use this command as a pod command instead of the original, can help debug complicated issues
- `debug_phase` - Debug mode for specific phase of pod events. All these events will be printed with the full response from k8s
- `prefix_remote_log` - Adds [driver] or [<task_name>] prefix to logs streamed from Kubernetes to the local log
- `check_unschedulable_condition` - Try to detect non-transient issues that prevent the pod from being scheduled and fail the run if needed
- `check_image_pull_errors` - Try to detect image pull issues that prevent the pod from being scheduled and fail the run if needed
- `check_running_pod_errors` - Try to detect running pod issues like failed ContainersReady condition (pod is deleted)
- `check_cluster_resource_capacity` - When a pod can't be scheduled due to CPU or memory constraints, check if the constraints are possible to satisfy in the cluster
- `startup_timeout` - Time to wait for the pod to get into the Running state
- `max_retries_on_log_stream_failure` - Determines maximum retry attempts while waiting for pod completion and streaming logs in --interactive mode. If set to 0 - no retries will be performed.
- `dashboard_url` - skeleton url to display as kubernetes dashboard
- `pod_log_url` - skeleton url to display logs of pods
- `pod_yaml` - Base YAML to use to run databand task/driver
- `trap_exit_file_flag` - trap exit file
- `auto_remove` - Auto-removal of the pod when the container has finished.
- `detach_run` - Submit run only, do not wait for its completion.
- `watcher_request_timeout_seconds` - How many seconds watcher should wait for events until timeout
- `watcher_recreation_interval_seconds` - How many seconds to wait before resurrecting watcher after the timeout
- `watcher_client_timeout_seconds` - How many seconds to wait before timeout occurs in watcher on client side (read)
- `log_pod_events_on_sigterm` - When receiving sigterm log the current pod state to debug why the pod was terminated
- `pending_zombies_timeout` - Amount of time we will wait before a pending pod would consider a zombie and we will set it to fail
- `zombie_query_interval_secs` - Amount of seconds we wait between zombie checking intervals. Default: 600 sec => 10 minutes
- `zombie_threshold_secs` - If the job has not heartbeat in this many seconds, the scheduler will mark the associated task instance as failed and will re-schedule the task.
- `airflow_log_enabled` - Enables Airflow Live log at KubernetesExecutor feature
- `airflow_log_image` - Override the image that will be used to add sidecar to the run which will expose the live logs of the run. By default, the main container image will be used
- `airflow_log_folder` - Specify the location on the airflow image (sidecar), where we mount the logs from the original container and expose them to airflow UI.
- `airflow_log_port` - The port airflow live log sidecar will expose its service. This port should match the port airflow webserver tries to access the live logs
- `airflow_log_trap_exit_flag_default` - The path that will be used by default if `airflow_log_enabled` is true
- `container_airflow_log_path` - The path to the airflow logs, on the databand container.
- `host_as_ip_for_live_logs` - Set the host of the pod to be the IP address of the pod. In Kubernetes normally, only Services get DNS names, not Pods. We use the IP for airflow webserver to lookup the sidecar See more here: https://stackoverflow.com/a/59262628

### Supported Kubernetes errors:

- `err_image_pull` happens on Image pull error.
- `err_config_error` happens on Pod configuration error (you should not retry on this one).
- `err_pod_deleted` happens on Pod deletion (very unique case of Kubernetes autoscaling).
- `err_pod_evicted` happens on Pod relocation to a different Node.

## FAQ

### Custom Configuration per Task

You can adjust configuration settings of a specific task:

```python
from dbnd import task

@task(task_config=dict(kubernetes=dict( limits={"nvidia.com/gpu": 1})))
def prepare_data_gpu(data):
     pass
```
Using this configuration, you'll add an extra limit to the pod definition of this specific task.

You can adjust requested resources and set the limits of memory and CPU:

```python
from dbnd import task
@task(
    task_config={
        "kubernetes": {"limit_memory": "128Mi",
                       "request_memory": "64Mi",
                       "limit_cpu": "500m",
                       "request_cpu": "250m"}
    }
)
def prepare_data_gpu(data):
    pass
```

You can also change engine config via CLI and even extend some values like `labels`and other properties with List type. See more information at [Task Configuration](doc:object-configuration) and [Extending Values](doc:extending-parameters-with-extend).


### Providing Access to AWS (using environment variables)

If you want to provide access to AWS Services explicitly, you can do it by using secrets:
```ini
[your_kubernetes_engine]
secrets = [   { "type":"env", "target": "AWS_ACCESS_KEY_ID", "secret" : "aws-secrets" , "key" :"aws_access_key_id"},
          { "type":"env", "target": "AWS_SECRET_ACCESS_KEY", "secret" : "aws-secrets" , "key" :"aws_secret_access_key"}]
```

### Providing Access to Google (using file)

If you want to provide access to GCP Services explicitly, you can do it by using secrets:
```ini
[your_kubernetes_engine]
secrets = [ { "type":"volume", "target": "/var/secrets/google", "secret" : "gcp-secrets" }]]
```
