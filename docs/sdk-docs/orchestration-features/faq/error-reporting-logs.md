---
"title": "Error Reporting Logs"
---
You can download the error logs for every run using Databand UI. With the help of the logs, you can access the command-line arguments that were executed and the log file for the actual task with the full log from the database.

Error logs make the monitoring of different states and task run attempts easier. They also simplify customer support as you can send your logs to Databand for troubleshooting without omissions, errors, missing database states, etc. 

Exporting logs is available for both [Tracking Quickstart](doc:tracking-quickstart) and [Orchestration Quickstart](doc:orchestration-quickstart) as this feature is global and covers an entire task run. 

### Downloading the logs
To download the error reporting logs for a run in a pipeline:
* Open the pipeline to see the run and open it

![Error reporting logs 01](https://files.readme.io/9af3ca4-Error-reporting-logs-01.png)

* In the upper right corner of Databand UI for that run, click the ‘Download Logs’ button. The web server will pack the logs for this run into a \*.tar.gz archive file that will be downloaded to your machine.

![Error reporting logs 02](https://files.readme.io/d272077-Error-reporting-logs-02.png)

The resulting \*.tar.gz archive file encapsulates an entire task run and contains logs of all the attempts at task execution in a separate folder with subfolders for task attempts, and a separate system task folder `dbnd_driver` (for the DBND SDK-performed tracking; for non-DBND runs, the name of the driver folder will be `orchestrator-name_driver`). 

![Error reporting logs 03](https://files.readme.io/bcfc1fc-Error-reporting-logs-03.png)

Let’s take a closer look at the logging artifacts. 

### System task folder (dbnd_driver)

`dbnd_driver` is the system task that runs all other tasks. Its log documents the entire Databand task run and Spark logs. Within `dbnd_driver`, you can find the number of task run attempts and states. 

The information within the `dbnd_driver` folder will provide the most accurate clues to the existing issues in general execution, such as issues with Kubernetes or Spark dependencies. The system task folder will only appear as`dbnd_driver` for pipelines that use the [DBND SDK orchestration](doc:orchestration-quickstart). 

Logs’ archive for tasks that were run on Airflow and are only tracked by DBND will not contain a dbnd_driver task. In this case, there will be an archive with folder(s) that describe(s) task(s) and a system \*_driver folder (for example, pyspark_driver). A \*_driver log folder also contains \*.log and a \*.cli files that help identify and resolve an issue. The names of such files match the task run attempt number.

If you switch to a specific task attempt log folder for this run, you’ll see a shorter, more concise log.

### Task run attempt folders and states
Task run attempt folders help identify the issues in specific tasks and make it easier to navigate the different steps of the execution, for example, a Python task that failed or some code that doesn’t work. 

There are three possible attempt states:
* failed - run attempt failed
* success - run attempt was successful
* canceled - run attempt was canceled

If a task was run three times, failed during the first two attempts, and eventually succeeded, you will see three attempt folders under this task - two in a ‘failed’ state and one that is a ‘success’. The number in the name of the folder is the task run attempt number. 

![Error reporting logs 04](https://files.readme.io/1342524-Error-reporting-logs-04.png)

All task run attempt folders contain \*.log and \*.cli files that help identify and resolve an issue. Logs for a specific task only contain the commands and the log for this particular task. The names of such files match the task run attempt number.

### CLI files
The \*.cli files in the log archive are text files with the list of the command-line arguments that were executed.

Example contents of a \*.cli file in a log archive: 
```
dbnd run dbnd_docker.docker.docker_build.DockerBuild --set docker_file=etc/build_docker/Dockerfile.dbnd_examples --set image_name=gcr.io/dbnd-dev-260010/databand --set tag=dbnd_build_20210127_105541 --env gcp_k8s --set task_target_date=2021-01-27 --set task_version=20210127_105559
``` 

These files are added to the log archive for convenience and are mostly used for context. A \*.cli file doesn’t describe the whole configuration, but it provides useful hints towards what was changed in the configuration. It also contains the ‘parent’ task - like the root pipeline that was executed. 

This is helpful for providing more clarity to a \*.log file as \*.cli files contain such small helpful pieces of information like configuration values. For example, there can be different Spark tasks, and the \*.cli file can provide the information on Python the wheel file used for the dependencies.  

### Log files
The \*.log files provide better observability and an overview of the execution, dividing it into smaller tasks and describing all the steps and events in the task run. There is one \*.log file per task (all of them are also contained in the dbnd_driver log, but they are divided by tasks in the task folders, making it easier to read and navigate the logs).  

Example \*.log file contents:

```bash
[2021-01-27 10:57:56,764] {tracking_store_console.py:103} INFO - 
[36m==================== 
[0m[36m= Running task dbnd_sanity_check__48f02c8ac2(dbnd_sanity_check)
[0m [1mTASK[0m       : task_id=dbnd_sanity_check__48f02c8ac2  task_version=1  env=gcp_k8s  env_cloud=gcp  env_label=dev  task_target_date=2021-01-27
 [1mTIME[0m       : start=2021-01-27 10:57:56.760983+00:00
 [1mTRACKER[0m    : http://databand-demo-develop.dbnd.local/app/jobs/dbnd_sanity_check/34bc8b52-608e-11eb-8982-acde48001122/541f05b0-608e-11eb-9bde-acde48001122
 [1mTASK RUN[0m   : task_run_uid=541f05b0-608e-11eb-9bde-acde48001122  task_run_attempt_uid=541f218a-608e-11eb-b5d2-acde48001122  state=TaskRunState.RUNNING
 [1mLOG[0m        : local=/Users/user/development/databand-delta/data/dbnd/log/2021-01-27/2021-01-27T105545.132695_dbnd_sanity_check_elastic_swirles/tasks/dbnd_sanity_check__48f02c8ac2/1.log  remote=gs://dbnd-dev-260010/dev/dbnd_root/dev/2021-01-27/dbnd_sanity_check/dbnd_sanity_check_48f02c8ac2/meta/attempt_1_541f218a-608e-11eb-b5d2-acde48001122/1.log.csv
 [1mPARAMS:[0m    : 
Name        Kind    Type      Format    Source                               -= Value =-
check_time  param   datetime            d.t.b.s.dbnd_sanity_check[defaults]  2021-01-27T125541.799523
result      output  str       .txt                                           "gs://dbnd-dev-260010/dev/dbnd_root/dev/2021-01-27/dbnd_sanity_check/dbnd_sanity_check_48f02c8ac2/result.txt"
[36m=
[0m[36m==================== 
[0m
[2021-01-27 10:57:56,767] {api_client.py:38} INFO - Webserver session does not exist, creating new one
[2021-01-27 10:57:56,767] {api_client.py:69} INFO - Initialising session for webserver
[2021-01-27 10:57:56,798] {api_client.py:85} INFO - Got csrf token from session
[2021-01-27 10:57:56,799] {api_client.py:89} INFO - Attempting to login to webserver
[2021-01-27 10:57:56,953] {sanity.py:15} INFO - Running Sanity Check!
[2021-01-27 10:57:56,954] {tracking_store_console.py:168} INFO - Task dbnd_sanity_check metric: Happiness Level=High
[2021-01-27 10:57:56,985] {sanity.py:17} INFO - Your system is good to go! Enjoy Databand!
[2021-01-27 10:57:57,290] {tracking_store_console.py:168} INFO - Task dbnd_sanity_check metric: marshalling_result=303.5738468170166
[2021-01-27 10:57:57,747] {tracking_store_console.py:98} INFO - Task dbnd_sanity_check__48f02c8ac2(dbnd_sanity_check) has been completed!
```

### Privacy considerations and recommendations
The information provided in the log archives might sometimes contain API tokens or other valuable information and credentials. This is why we do not recommend putting such information into the CLI, as a general rule.

If you absolutely need to, manually delete the sensitive information from the logs before sending the logs to Databand for troubleshooting.