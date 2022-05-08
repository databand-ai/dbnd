---
"title": "FAQ and Troubleshooting"
---
### Q: How to debug pipeline execution without actually running it?
A: Using `--describe` flag for the command line would run the task/pipeline without executing it.

For example:
```bash
dbnd run prepare_data --verbose --describe
```
You can combine it with `--verbose` to see more logs and `--disable-web-tracker` to run without reporting to the webserver.

If you submit a task to Kubernetes, add `--local-driver` to make sure that you run the driver on the local machine and it is shown in the `describe` output.

### Q: How can I debug my task parameters and signature calculation?
A: Using `--verbose` flag for the command line would print extra information on task parameters.
Use `-v -v` (verbose=2) to get the full info about the task object to build.

For example:
```bash
dbnd run prepare_data --verbose --verbose
```
You can combine it with `--verbose` to see more logs and `--disable-web-tracker` to run without reporting to the webserver.

### Q: How can I run my task without sending information to Web UI?
A: You can do that using `--disable-web-tracker`  flag.