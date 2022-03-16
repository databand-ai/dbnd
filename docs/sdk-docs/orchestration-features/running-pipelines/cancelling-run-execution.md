---
"title": "Cancelling Run Execution"
---
Canceling runs can be done externally and internally.

To kill a run execution from within execution code:

```python
    from dbnd import cancel_current_run
    cancel_current_run()
```

To kill a run execution, outside of execution code:

```python 
    databand_run.kill_run()
```

## Setting Custom Error Message

It is possible for to set a custom error message when you use the `kill_run` functionality. The resulting error message can be viewed on the Runs page.
Example:
```python
    from dbnd import cancel_current_run
    cancel_current_run(message="custom cancel message")
```
or
```python 
    databand_run.kill_run(message="custom cancel message")
```
Since you could have had different reasons for killing a run (for example, you identified corrupt data, reached a resource limit, etc.), setting a custom error message allows you to differentiate between the occasions in which you killed their run, for future analysis.