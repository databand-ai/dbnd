---
"title": "Kubernetes Integration"
---
## Before You Begin

First, you need to configure DBND [configure DBND](doc:kubernetes-engine-configuration).
Setup your [K8S cluster](doc:setup-k8s-cluster)

When you have the Kubernetes cluster and DBND installed, you can make it run.


### Verify the Integration with the Kubernetes Cluster
1. Run the sanity check task on the Kubernetes cluster:
```bash
dbnd run dbnd_sanity_check --task-version now --env kubernetes_cluster_env --interactive
```

In this command, we used the following parameters:

`-- env kubernetes_cluster_env` is the environment configuration section.
`--interactive` flag ensures that we will receive all logs from the pods, as well as wait until the run is complete.

2. To ensure that the task succeeds, run the `kubectl get\describe pods` command.

>🚧 Note
> If you defined a new configuration section and are trying to run an already-built DBND image, you might have to compile it by yourself with the new configuration section.

## Kubernetes Lifecycle Overview
When you execute a task by using the Kubernetes engine, you initiate a lifecycle for each task.
The lifecycle depends on your configuration. In the sections above, we describe the main lifecycles.

The following CLI switches are useful when you run commands on a Kubernetes cluster:

| Command | Description |
|---|---|
| --interactive | Runs tasks submission in blocking mode. Ensures that the task submitting the smaller tasks (driver) will be run locally. This way you can debug how your pods are created. Waits until the execution is complete and displays all logs for all pods. |
| --submit-driver | Runs a driver on a remote engine. The driver task (that submits all other tasks) is run remotely inside the Kubernetes pod. |
| --local-driver | Runs a driver on a local driver.  Driver task will be run locally. This way you can debug how your pods are created. |
| --submit-tasks | Submits tasks from a driver to a remote engine. |
| --no-submit-tasks | Disables tasks submission so that tasks run in the driver process. |
| --docker-build-tag | Defines a custom Docker image tag for the Docker image to be be built |



### Submitted Driver
Take a look at the following example:

```bash
dbnd run dbnd_sanity_check --submit-driver --task-version now --env kubernetes_cluster_env
```

The `--submit-driver` flag affects where our driver runs, and hence affects the lifecycle.
We run through the following algorithm:

1.  Build a driver task.
2. Gather all the needed resources and settings to create a pod with a driver task inside.
-> The pod creation request is sent to the cluster.
3. Submit the driver task.
 -> The driver task is sent to the Kubernetes and is ran inside the pod. The driver task creates a new pod to run user code.
4 .Run user tasks within a pod.
-> The user tasks are executed inside the cluster.
-> When the user code starts running, the driver gathers output, logs, etc. of the task that was executed. It is in charge of running the next pod when the current pod finishes.
-> The driver executes the next task until the end of DAG

### Local Driver
Now let's look at the following command:

```bash
dbnd run dbnd_sanity_check --local-driver --task-version now --env kubernetes_cluster_env
```
In this command, the `--local-driver` flag ensures that our driver runs locally and not inside the cluster.

Now, the lifecycle is the following:

1. Build a driver task and gather all the necessary resources for the driver.
2. Run the driver task locally.
-> The driver task is not sent to Kubernetes and is executed on the local engine. The driver still sends pods to in-cluster execution.
3. The user code runs within pods. User tasks are executed inside the cluster.
4. When the user code starts running, the driver gathers the logs, outputs, etc. It is in charge of running the next pod when the current pod finishes.
5. The driver executes the next task until the DAG is completed.


### Advance Example
```bash
dbnd run dbnd_sanity_check  --task-version now --env=kubernetes_cluster_env --interactive --set-config kubernetes.namespace=my_namespace  --set-config kubernetes.container_tag=dbnd-dbnd_orchestration_examples-py39-branch-release --set kubernetes.debug=true
```

Let's break it down:

- `dbnd run dbnd_sanity_check --task-version now` - Basic dbnd command.
- `-env=kubernetes_cluster_env` - The environment we want to run with, which is configured to use the k8s engine
- `-interactive` - Tells the submitter (local machine) to wait for the run to end.
- `-set-config kubernetes.namespace=my_namespace` - The namespace we are using.
- `-set-config kubernetes.container_tag=dbnd-dbnd_examples-py39-branch-release` - The image we want to use, built in the same pipelines as the env.
- `-set kubernetes.debug=true` - debug flag for kubernetes Executor

## Troubleshooting Kubernetes Cluster
When you run your code, you might encounter different issues. DBND provides you with a set of tools that help you understand the underlying cause of a problem.  See [Kubernetes Troubleshooting](doc:kubernetes-troubleshooting)

| Configuration Parameter | Description |
|---|---|
| kubernetes.keep_finished_pods=True | When set to `True`, it never deletes any pods. It is very useful when you try to understand what went wrong inside a pod's execution. |
| kubernetes.debug=True | Prints all the debug information, most importantly pod events that we receive. |
