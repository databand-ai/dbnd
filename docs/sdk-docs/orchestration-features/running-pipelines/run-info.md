---
"title": "Run Info"
---
DBND collects the run info on every Run and makes it available at UI.  Accessing the information about the run context can be very useful - for example, for finding the name of the user that is running the pipeline.

`git version` (VCS), pod name, user name, and many other properties are going to be collected at the moment of the Driver execution.

Accessing this information is available through the `task_run_env` property of `task_run` . It includes:

* `user` - the user name can be configured through the `run_info.user` config
* `databand_version` - databand SDK version you are using
* `user_code_version` - user's git commit of the code that is currently running
* `cmd_line` - the command executing this Run
* `project root` - the Databand project root directory
* `user_data` - custom user data option, can be configured through the `run_info.user_data` config
* `source_version` - gather version control via git/None


### VCS value column is not displayed for some jobs

VCS value is defined via `run_info.source_version` variable. The default value is `git`, which is resolved into the current GIT HEAD of the current folder. If you want to provide your own value, you can either:
* do it via `--set run_info.source_version=YOUR_VCS_VALUE` or
* via `export DBND__RUN_INFO__SOURCE_VERSION=YOUR_VCS_VALUE`

If your code is baked into a Docker image without a `.git` folder, you can embed your VCS version into your Docker image. To do it:
1. Change your Docker file to include:
 `ENV DBND__RUN_INFO__SOURCE_VERSION=${SOURCE_VERSION}`
2. Build the image with extra args: `--build-args SOURCE_VERSION=YOUR_VCS_VALUE`
