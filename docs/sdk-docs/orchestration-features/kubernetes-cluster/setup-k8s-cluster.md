---
"title": "Setup Kubernetes Cluster"
---
You must have a running Kubernetes cluster (local [Minikube](https://minikube.sigs.k8s.io/docs/start/), or production cluster).

The code running inside the pod needs credentials and connection strings to access DBND and Airflow, so you must define the secrets.

To create the required `databand-secrets`  secrets, run the following command:

```bash
kubectl create secret generic databand-secret \
--from-literal=airflow_fernet_key=AIRFLOW_FERNET_KEY \
--from-literal=airflow_sql_alchemy_conn=AIRFLOW_SQL_ALCHEMY_CONN \
--from-literal=databand_url=DATABAND_URL
```
```Parameter
sql_alchemy_conn is the connection string to the database
fernet_key is the encryption key for Airflow
databand_url is the Url of the Databand webserver
```

fernet_key and sql_alchemy_conn are **Airflow** connections accessible from the pod
databand_url is ** Databand URL **, accessible from the cluster

Make sure that settings specified in your dbnd and airflow configuration match the `databand-secrets` inside the cluster.

For the example above you should have following definition at your [Kubernetes Engine Configuration](doc:kubernetes-engine-configuration) 
```buildini
[kubernetes_engine] # make sure it's your engine name

secrets = [
   { "type":"env", "target": "AIRFLOW__CORE__SQL_ALCHEMY_CONN", "secret" : "databand-secret", "key": "airflow_sql_alchemy_conn"},
   { "type":"env", "target": "AIRFLOW__CORE__FERNET_KEY", "secret" : "databand-secret", "key": "airflow_fernet_key"},
   { "type":"env", "target": "DBND__CORE__DATABAND_URL", "secret" : "databand-secret", "key": "databand_url"}
   ]


```

>📘	Kubernetes commands to manage secrets
> `kubectl get secret databand-secrets`
`kubectl edit secret databand-secrets`
`kubectl delete secret databand-secrets`


## Image pulling
For remote repositories, you must supply the cluster with an image pulling secrets to ensure the Docker inside the pod has access to the repository. You can configure pull-secret at [Kubernetes Engine Configuration](doc:kubernetes-engine-configuration) 

See [this article](https://medium.com/hackernoon/today-i-learned-pull-docker-image-from-gcr-google-container-registry-in-any-non-gcp-kubernetes-5f8298f28969) for further documentation about image pull secrets.