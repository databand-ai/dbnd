---
"title": "Add Python Project to spark submission"
---
## Automatically add Python Packages to Spark Submission

You can upload external packages to Spark by using `spark.include_user_project=True` [Spark Configuration](doc:spark-configuration)
DBND supports configuring a package directory (which contains its `setup.py`) and an optional third-party requirements text file.

Example:

```
[ProjectWheelFile]
package_dir=${DBND_HOME}/dbnd-core/orchestration/dbnd-test-scenarios/scenarios/dbnd-test-package
requirements_file=${DBND_HOME}/dbnd-core/orchestration/dbnd-test-scenarios/scenarios/dbnd-test-package/requirements.txt
```

>ℹ️ Important
> The packages will be built and uploaded every time a Spark task needs to be rerun.
>
> Thus, for example, if you are running a pipeline for the second time, and all of its tasks are reused, then nothing will happen. But if the signature changes in at least one of the Spark tasks, it will rerun, and the packages will be rebuilt and reuploaded.
