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

    Parameter `_from` means "from where to draw previous definitions". In this example, the `[local]` section is used (see [Extending Configurations](doc:custom-environment)).
    The `remote_engine` setting defines what engine is going to run your remote tasks (submitted tasks).

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

## Databand Kubernetes Config Reference
## Docker Image configuration
-   `container_repository` - Where is the Docker image repository to pull the pod images from? If you are running user code, this is where you need to supply your repository and tag settings. 
-   `container_tag` - If defined, Docker will not be built and the specified tag will be used.
-   `image_pull_secrets` - The secret with the connection information for the `container_repository`.
-   `docker_build` - Should the Kubernetes executor build the Docker image on the fly? Useful if you want a different image every time.
-   `docker_build_push` - Should the built Docker image be pushed to the repository? Useful for specific cases.

### Cluster related variables
-   `in_cluster` - Defines what Kubernetes configuration is used for the kube client.  Use `false` to enforce using local credentials, use `true` to enforce the `in_cluster` mode.  Default: `None` (Databand will automatically decide what mode to use).
- `cluster_context` - The Kubernetes context; you can check which context you are on by using `kubectl config get-contexts`.
-   `namespace` - The namespace in which Databand is installed inside the cluster (`databand` in this case).
- `service_account_name` - You need permissions to create pods for tasks, namely -  you need to have a `service_account` with the correct permissions.

### Pod Scheduling Configuration
- `labels` - Set a list of pods' labels (see [Labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/))
- `node_selectors` and `affinity` - Assign `nodeSelector` or `affinity` to the pods (see [Assigning Pods to Nodes](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/))
- `annotations` - Assign annotations to the pod (see [Annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)) 
- `tolerations` - Assign tolerations to the pod (see [Taints and Tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/))
-  `requests` and `limits` - Setting the requests and limits for the pod can be achieved by setting those. You can provide a standard Kubernetes Dict, however, you can also use explicit keys like `request_memory` , `request_cpu`, `limit_memory` or `limit_cpu`
For more information see [Manage Container Resources](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/) and make sure you are aware of [Quality of Service for Pods](https://kubernetes.io/docs/tasks/configure-pod-container/quality-service-pod/)

### Pod Runtime Configuration
-   `secrets` - Assing secrets to the pod.
 -   `env_vars` - Assign environment variables to the pod.

### Pod Error Handling
-   `pod_error_cfg_source_dict` (optional) - Allows flexibility of sending retry on pods that have failed with specific exit codes.  You can provide "PROCESS EXIT CODE" as a key (for example, `137`) or Kubernetes error string.
 
```  
pod_error_cfg_source_dict = {
                                "255": {"retry_count": 3, "retry_delay": "3m"},
                                "err_image_pull": {"retry_count": 0, "retry_delay": "3m"},
                                "err_pod_deleted": {"retry_count": 3, "retry_delay": "3s"},
                                }
```

Supported Kubernetes errors: 

*  "err_image_pull" happens on Image pull error.  
* "err_config_error" happens on Pod configuration error (you should not retry on this one).
* "err_pod_deleted" happens on Pod deletion (very unique case of Kubernetes autoscaling). 
* "err_pod_evicted" happens on Pod relocation to a different Node.

### Databand System 
-   `debug` - When true, displays all pod requests sent to Kubernetes and more useful debugging data.
-  `keep_finished_pods` - do not delete finished pods (default=False) 
-  `keep_failed_pods` - do not delete failed pods (default=False). You can use it if you need to debug the system.    
-   `_type` - Implies that this is a Kubernetes Engine Config (see [Extending Configurations](doc:custom-environment)). You can use it to create your own version of the Kubernetes Engine config. 

## FAQ

### Custom Configuration per Task

You can adjust configuration settings of a specific task:

```python
@task(task_config=dict(kubernetes=dict( limits={"nvidia.com/gpu": 1})))
def prepare_data_gpu(data):
     ...
```
Using this configuration, you'll add an extra limit to the pod definition of this specific task.

You can adjust requested resources and set the limits of memory and CPU:

```python
@task(
    task_config={
        "kubernetes": {"limit_memory": "128Mi",
                       "request_memory": "64Mi",
                       "limit_cpu": "500m",
                       "request_cpu": "250m"}
    }
)
def prepare_data_gpu(data):
     ...
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