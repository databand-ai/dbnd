# History

## DBND v 0.34.00

### New features:

- \#1199382295296733: Projects support
- \#1199503238625634: Support user access token
### Improvements:

- \#1199410590823866: Using pipeline(=band) outputs in runtime
### Fixed issues:

- \#1199512537243676: Enable pod logs collection with google logs
- \#1199511121032309: Zombie Driver at GKE - unsynced state
- \#1199538657255959: Link at Error banner at Details page looks broken
- \#1199624982127115: Configuration per task run and not all pipeline run
- \#1199560164027872: Zombie Drive at GKE - watcher process not dying on  driver termination
- \#1199665645625989: fix Marshalling of nested lists
- \#1199692629063013: log_dataframe(...,with_stats=True) does not print stats to CLI

## DBND v 0.33.00

### New features:

- \#1199539989855853: silence logging on tracking in production
- \#1199410183946263: [java sdk] log collection - bring both head and tail
- \#1186815542031177: add @data_source_pipeline annotation

### Improvements:

- \#1199601902803965: Default delay is set to 1 for log_snowflake_resource_usage
- \#1183486068581313: auto-add dbnd spark listener
- \#1195941695830497: Clean console outputs in tracking scenario

### Fixed issues:

- \#1199534865809603: fix DbndSchedulerDBDagsProvider retries
- \#1199553897458772: Pendulum fix for log_metric
- \#1199592885591350: EKS(AWS) submitter log read timeout after 4h
- \#1199383391833788: Normalize

## DBND v 0.32.00

### Improvements:

-   \#1199513772139741: read azkaban job properties from file
-   \#1199222341211949: log_metric() is compatible with BIGINTs
-   \#1199191513095141: remove git working from dbnd log
-   \#1199352826744440: Better run name and job name for azkaban
-   \#1199362041298460: add Flow/Job properties to azkaban tracking
-   \#1198203153122085: introduce airflow uid (sync and execute)

### Fixed issues:

-   \#1199593868894397: Sync monitor - bad "from"
-   \#1199562298463958: fix expire_heartbeat query
-   \#1199561667659647: Move ScheduledJob, RunState updates are done from for loop to bulk
-   \#1199561667659637: Improve Airflow monitor performance
-   \#1199360347022221: fix dbnd db init printouts

## DBND v 0.31.00

### Improvements:

-   \#1199351713607668: log collection - bring both head and tail
-   \#1199222341211949: make log_metric() compatible with BIGINTs
-   \#1199191513095141: remove git working from log
-   \#1199352826744440: Better run name and job name for Azkaban

### Fixed issues:

-   \#1185047487019177: add dataframe name to target reporting with log_dataframe()
-   \#1198956007770574: fix jvm tracking timeouts

## DBND v 0.30.04

### Fixed Issues:

-   \#1196324185480705: make databand task return proper result
-   \#1198935333953725: fix pickling compatibility issue for fat wheel.

## DBND v 0.30.00

### New features:

-   \#1195948952063399: Azkaban support
-   \#1198875598805805: Support dbnd_cache for multiple scenarios

### Improvements:

-   \#1199199113928119: Documentation for Snowflake Resources Operator
-   \#1185230434439866: Mark task that received signal (sigterm?) with different color/state
-   \#1199191096927422: snowflake: sleep for resource fetch
-   \#1199109359508985: improve performance of airflow-monitor
-   \#1199135775563013: add cli to manage airflow monitors
-   \#1188613755173249: lazy read for task params

### Fixed issues:

-   \#1199195747557789: improve performance on relation fetching
-   \#1199183015685020: fix nested run issue
-   \#1198912139427114: fix airflow logging
-   \#1198951004489095: task_visualizer banner - configurable max value size

## DBND v 0.29.00

### New features:

-   \#1198182294540228: Add Deeque support
-   \#1196095832101313: Define alerts cli
-   \#1198215411158397: Standalone tracking and track_operator method

### Improvements:

-   \#1196565777511077: Live logging for all ad-hoc tasks
-   \#1189443575273138: JVM SDK Logging
-   \#1189866958488326: Add separate config section for tracking
-   \#1195079839215316: Support tensorflow 2.0
-   \#1195941695824501: Support Airflow DAG changes / deletions
-   \#1198172260918388: Helm - support any env variables block( common for all our services)
-   \#1125138619443457: Command li Add flag to command line
-   \#1198199342753981: move require local sync to parameter from target config
-   \#1194785058398298: track airflow templated parameters

### Fixed issues:

-   \#1198178723258493: Task-level Airflow link leads to the wrong destination.
-   \#1179311676368701: Too old resource version on k8s
-   \#1177515786548674: support MultiTarget with require_local_access=true
-   \#1194826730548337: Handle incomplete output scenario

## DBND v.0.28.26

### New features:

-   \#1195944991344117: add dbnd-snowflake module for tracking snowflake data and metrics
-   \#1196277825236673: support RBAC for export_plugin

### Improvements:

-   \#1176447682585577: support basic authentication with livy
-   \#1194347540945401: add rendered xcom to task params
-   \#1194347540945407: report Airflow DAG params
-   \#1195924719351296: report Airflow operator params

### Fixed issues:

-   \#1193537416358473: column type not shown when histograms = true but statistics=False
-   \#1188126160239994: histograms for pandas on empty column produce an error in a log

## DBND v.0.28.20

### New features:

-   \#1191658141334873: Make dbnd-luigi workable

### Improvements:

-   \#1191868676285987: calculate spark descriptive statistics efficiently
-   \#1192562605942417: Logging in Jupyter Notebok
-   \#1187220511775489: report target load/save/download time as a metric
-   \#1192948860705935: fat_build_task inherits target_date from parent
-   \#1164673115500855: simplifying DBND spark implementation ( no airflow)

### Fixed issues:

-   \#1193537416358473: column type not shown when histograms = true but statistics=False
-   \#1188126160239994: histograms for pandas on empty column produce an error in a log

## DBND v.0.28.13

### New features:

-   \#1190345801510096: report user params for Airflow operators

###Improvements:

-   \#1191658141334875: Better exception handling to prevent failing DAG retireval from versioned-dag
-   \#1188403453583067: HDFS local folder open after pyspark

### Fixed issues:

-   \#1190536560972406: airflow skipped task has two run attempts in databand db
-   \#1192515450649121: spark histogram error: list indices must be integers or slices, not NoneType
-   \#1191864220891825: histogram calculation fails on complex type
-   \#1190570128931897: Solve compatibility errors of versioned dag and airflow 1.10.10

## DBND v.0.28.6

### Improvements:

-   \#1171185232799219: support --set run.task
-   \#1184271040807809: Parallelize checking if tasks are complete
-   \#1189508556532473: support airflow 1.10.10
-   \#1183936916078880: Retrieve spark exception when running spark locally

## DBND v.0.28.05

### New features:

-   \#1186838986890801: Created @log_duration to measure and log execution time as a metric

### Improvements:

-   \#1186434551886169: Log pandas histogram calculation time as a metric
-   \#1183936916078873: Display JSON path as well as content in preview
-   \#1185884345508633: Add Livy url to external links
-   \#1183936916078876: Retrieve Qubole traceback and exception when run fails
-   \#1187615130069532: ci/cd deploy dbnd to quoble/emr from s3 bucket
-   \#1184859956507691: airflow monitor: print proper error message on RBAC
-   \#1187144661730497: Added airflow icon to DAG and Operator levels of the graph
-   \#1187144661730508: Show recent metric values when selected anomaly detection alert
-   \#1186620728186105: Link from alert list modal to specific run details page

### Fixed issues:

-   \#1178184224046201: Tracking is slowing down tasks when dbnd-web address is wrong
-   \#1188853509921970: Add task-version now to fat_wheel building task
-   \#1188398439358980: Bring back loading of .dbnd/databand-test.cfg if exist
-   \#1187836261701377: Fix kubernetes infinite loop on change_state -> delete_pod
-   \#1188150247653234: support py-files with inline in all engines
-   \#1189443575273132: Fixed spark and java task graph node icons
-   \#1186213205378244: Run list table style alignment
-   \#1187782447252866: Pandas fails to write parquet file to hdfs

## DBND v.0.28.01

### New features:

-   \#1184635774415961: Added ability to disable automatic tracking on functions, decorators, and DAGs
-   \#1186666107312362: Warning message displayed when not all outputs are available for a task.
-   \#1184186845180735: Support for py_files in the Databricks environment.
-   \#1185262786942811: Added overwriting of targets via save_options to historically overwrite new data to the same path.
-   \#1184836053179881: Added ability to configure histograms

### Improvements:

-   \#1186033707483059: Trackers added to a task/pipeline run banner
-   \#1185174851651484: Tracking logic moved to dbnd_airflow
-   \#1183947756269387: Changed the default SparkDataFrameToCsv marshaller behaviour
-   \#1186029239496897: Added automatic logging for Spark initialization
-   \#1180558848817017: Automatic configuration of the databand_url on Spark
-   Timeline - parent tasks now clearly show when a child task was running
-   Show value inline for short params and results instead of the path or a placeholder

### Fixed issues:

-   \#1186251402357473: --disable-web-tracker function does not work.
-   \#1183600904070465: 500 HTTP error thrown on bad paramater definition: Format=parameter.choices(["artemis","bago_dialer"]).default("artemis")
-   \#1186666107312349: Recreated filesystem objects on remote execution to prevent credentials conflict
-   Timeline - children didn't always appear below their parents
-   Timeline - tasks with the same name were incorrectly grouped together
-   Refresh button wasn't working correctly on several pages
-   Recent values in alert config showing non-recent values
-   fixed run list UI issues
-   fixed task sometimes not being selected in the graph during initial page load
-   large performance improvements for the stats endpoint exporting metrics to prometheus

## DBND v.0.28.00

### New features:

-   Support for fat_wheel for Spark py_files

### Improvements:

-   New method for canceling pipelines
-   Processing tasks all output
-   Added ability to move objects across multiple environments
-   Logging marshalling operations in DBND
-   New pods retry behavior for pipelines running on Kubernetes
