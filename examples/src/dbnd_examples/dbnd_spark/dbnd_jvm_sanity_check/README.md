# Databand sanity check

## How to run

`./run.sh`

## Excepted output example

```
[INFO] SanityCheck$:16 - Databand server set to http://localhost:8080
[INFO] SanityCheck$:17 - Probing databand url...
[INFO] SanityCheck$:21 - Connection to databand established successfully
[INFO] SanityCheck$:29 - Creating pipeline...
Running Databand!
TRACKER URL: http://localhost:8080
[INFO] DbndClient:133 - [task_run: ...] Run created
[INFO] DbndClient:236 - [task_run: ...] Completed
Running pipeline jvm_sanity_check
[INFO] DbndClient:279 - [task_run: ...] metrics submitted: [time]
[INFO] DbndWrapper:201 - Metric logged: [time: 1.603999000000E12]
[INFO] DbndClient:279 - [task_run: ...] metrics submitted: [status]
[INFO] DbndWrapper:201 - Metric logged: [status: ok]
[INFO] DbndClient:309 - [task_run: ...] task log submitted
[INFO] DbndClient:214 - [task_run: ...] [task_run_attempt: ...] Updated with status SUCCESS
[INFO] DbndClient:236 - [task_run: ...] Completed
[INFO] SanityCheck$:37 - Pipeline stopped

```

