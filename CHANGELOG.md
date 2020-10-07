# History

DBND v.0.28.26

New features:

---
- #1195944991344117: add dbnd-snowflake module for tracking snowflake data and metrics
- #1196277825236673: support RBAC for export_plugin

Improvements:

---
- #1176447682585577: support basic authentication with livy
- #1194347540945401: add rendered xcom to task params
- #1194347540945407: report Airflow DAG params
- #1195924719351296: report Airflow operator params


Fixed issues:

---
- #1193537416358473: column type not shown when histograms = true but statistics=False
- #1188126160239994: histograms for pandas on empty column produce an error in a log


DBND v.0.28.20

New features:

---
- #1191658141334873: Make dbnd-luigi workable

Improvements:

---
- #1191868676285987: calculate spark descriptive statistics efficiently
- #1192562605942417: Logging in Jupyter Notebok
- #1187220511775489: report target load/save/download time as a metric
- #1192948860705935: fat_build_task inherits target_date from parent
- #1164673115500855: simplifying DBND spark implementation ( no airflow)

Fixed issues:

---
- #1193537416358473: column type not shown when histograms = true but statistics=False
- #1188126160239994: histograms for pandas on empty column produce an error in a log

DBND v.0.28.13

New features:

---
- #1190345801510096: report user params for Airflow operators

Improvements:

---
- #1191658141334875: Better exception handling to prevent failing DAG retireval from versioned-dag
- #1188403453583067: HDFS local folder open after pyspark

Fixed issues:

---

- #1190536560972406: airflow skipped task has two run attempts in databand db
- #1192515450649121: spark histogram error: list indices must be integers or slices, not NoneType
- #1191864220891825: histogram calculation fails on complex type
- #1190570128931897: Solve compatibility errors of versioned dag and airflow 1.10.10


DBND v.0.28.6

Improvements:

---
- #1171185232799219: support  --set run.task
- #1184271040807809: Parallelize checking if tasks are complete
- #1189508556532473: support airflow 1.10.10
- #1183936916078880: Retrieve spark exception when running spark locally


DBND v.0.28.05

New features:

---

-   #1186838986890801: Created @log_duration to measure and log execution time as a metric

Improvements:

---

-   #1186434551886169: Log pandas histogram calculation time as a metric
-   #1183936916078873: Display JSON path as well as content in preview
-   #1185884345508633: Add Livy url to external links
-   #1183936916078876: Retrieve Qubole traceback and exception when run fails
-   #1187615130069532: ci/cd deploy dbnd to quoble/emr from s3 bucket
-   #1184859956507691: airflow monitor: print proper error message on RBAC
-   #1187144661730497: Added airflow icon to DAG and Operator levels of the graph
-   #1187144661730508: Show recent metric values when selected anomaly detection alert
-   #1186620728186105: Link from alert list modal to specific run details page

Fixed issues:

---

-   #1178184224046201: Tracking is slowing down tasks when dbnd-web address is wrong
-   #1188853509921970: Add task-version now to fat_wheel building task
-   #1188398439358980: Bring back loading of .dbnd/databand-test.cfg if exist
-   #1187836261701377: Fix kubernetes infinite loop on change_state -> delete_pod
-   #1188150247653234: support py-files with inline in all engines
-   #1189443575273132: Fixed spark and java task graph node icons
-   #1186213205378244: Run list table style alignment
-   #1187782447252866: Pandas fails to write parquet file to hdfs

DBND v.0.28.01

New features:

---

-   #1184635774415961: Added ability to disable automatic tracking on functions, decorators, and DAGs
-   #1186666107312362: Warning message displayed when not all outputs are available for a task.
-   #1184186845180735: Support for py_files in the Databricks environment.
-   #1185262786942811: Added overwriting of targets via save_options to historically overwrite new data to the same path.
-   #1184836053179881: Added ability to configure histograms

Improvements:

---

-   #1186033707483059: Trackers added to a task/pipeline run banner
-   #1185174851651484: Tracking logic moved to dbnd_airflow
-   #1183947756269387: Changed the default SparkDataFrameToCsv marshaller behaviour
-   #1186029239496897: Added automatic logging for Spark initialization
-   #1180558848817017: Automatic configuration of the databand_url on Spark
-   Timeline - parent tasks now clearly show when a child task was running
-   Show value inline for short params and results instead of the path or a placeholder

Fixed issues:

---

-   #1186251402357473: --disable-web-tracker function does not work.
-   #1183600904070465: 500 HTTP error thrown on bad paramater definition: Format=parameter.choices(["artemis","bago_dialer"]).default("artemis")
-   #1186666107312349: Recreated filesystem objects on remote execution to prevent credentials conflict
-   Timeline - children didn't always appear below their parents
-   Timeline - tasks with the same name were incorrectly grouped together
-   Refresh button wasn't working correctly on several pages
-   Recent values in alert config showing non-recent values
-   fixed run list UI issues
-   fixed task sometimes not being selected in the graph during initial page load
-   large performance improvements for the stats endpoint exporting metrics to prometheus

DBND v.0.28.00

New features:
–––––––––––––

-   Support for fat_wheel for Spark py_files

Improvements:

-   New method for canceling pipelines
-   Processing tasks all output
-   Added ability to move objects across multiple environments
-   Logging marshalling operations in DBND
-   New pods retry behavior for pipelines running on Kubernetes
