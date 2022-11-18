---
"title": "dbnd-doctor Checks & Logs"
---
## General Diagnostics
DBND has a built-in `@task` that runs numerous checks on your environment.

Use the following command to run the diagnostic task:
```bash
dbnd run dbnd_doctor
```

## Troubleshooting Logs

In order to print the logging state of the system, please run the following command
```bash
dbnd run dbnd.tasks.doctor.system_logging.logging_status --task-version now
```
>if you are running using [Kubernetes Integration](doc:kubernetes-cluster), please also run `--no-submit-tasks` in addition to the run described above. This will execute all the diagnostic code at the "driver" pod as well.


> use `--disable-web-tracker` if the Databand Service is not available at the moment