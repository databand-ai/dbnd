---
"title": "Kubernetes Troubleshooting"
---
### Executing into Containers
Sometimes, the pod execution logs are not available if a pod fails before redirecting its logs. To understand these errors, you need some advanced troubleshooting.

One of the methods we use is building our pod as usual, but giving it a very lengthy command, such as `sleep`. While a pod is sleeping, you can investigate its container.

The following steps describe how you can troubleshoot the `databand_examples` pipeline running in the Kubernetes cluster.

1. Run a task that does nothing but sleeps for a long time. 
In the following example, we run a task named `long_task` that sleeps for 600 seconds.
```bash
dbnd run dbnd_test_scenarios.scheduler_scenarios.long_task --task-version now --env kubernetes_cluster_env
```
Your pod is now successfully initialized into a sleeping state.

2.  Find out the ID of the container where the `long_task` runs:
```bash
docker ps | grep "long"
```

3. Run bash inside the container:

```bash
docker exec -it <CONTAINER_ID> /bin/bash
```

Now, you have a shell on the pod. 

4. Run a command that fails, and investigate the issue.

>✅ Tip
> You can get the command line that fails from the log of the driver task.

### Pod Creation Errors 
`databand-secrets-env`, which is deployed when you install DBND, includes `cluster-role-binding` that provides your driver with the necessary permissions to execute pods inside your namespace.  If it fails for any reason, your driver will report errors on pods creation. You must ensure that it has the required permissions.