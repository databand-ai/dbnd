---
"title": "Accessing Collected Data and Managing Databand"
---
You can use our client to access data collected by our DBND library.
It is a Python-based client that you can use to easily access all runs information, tasks and more...

#Before we start
Make sure you have your sdk installed ( [Installing DBND](doc:installing-dbnd)  ), configured ( [SDK Configuration](doc:dbnd-sdk-configuration) ] ), and connected to the web-server ([Connecting DBND to Databand (Access Tokens)](doc:access-token)).

#How to use it?
To use the Databand client, you are going to need to use `DatabandClient`'s `build_databand_client`, which is being created from the execution's Databand context.
## Getting Run Info
In the following example we are getting detailed run information, of run with run uid: "example-run-uid", which also includes detailed data of the task runs in that run, and each task run attempts.


<!-- noqa -->
```python
from dbnd.api.databand_client import DatabandClient

client = DatabandClient.build_databand_client()
run_info = client.get_run_info(run_uid="example-run-uid")
```

## Finding all attempts and their errors
What you need to do is use the `get_run_info` method, with the wanted run_uid.
The returned value holds task_runs property, which is a list of all the task runs in that run, there you have a `latest_error` field, which is the latest error of the last task run attempt. If you want to check the error across the different attempts of that task, you have a `task_run_attempts` field, which has a `latest_error` field for every attempt.


<!-- noqa -->
```python
from dbnd.api.databand_client import DatabandClient

client = DatabandClient.build_databand_client()
run_info = client.get_run_info(run_uid="example-run-uid")
for task_run in run_info["task_runs"]:
    # Option 1
    latest_error = task_run["latest_error"]
    # Do Something

    # Option 2
    for task_run_attempt in task_run["task_run_attempts"]:
        attempt_error = task_run_attempt["latest_error"]
        # Do Something
```

## Get error cause of a run
In order to get the error that cause a run to fail, you can use the `get_first_task_run_error` method, with the desired run_uid.
This method is looking for the first task run that failed, and returns the latest error of that task run, what you get is a representation of the error, most notably:`msg` as the original error message.


<!-- noqa -->
```python
from dbnd.api.databand_client import DatabandClient

client = DatabandClient.build_databand_client()
run_error = client.get_first_task_run_error("example-run-uid")
# to get the error message
error_msg = run_error["msg"]
```