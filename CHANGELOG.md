History
=======

DBND v.0.28.05

New features:
____________
- #1186838986890801: Created @log_duration to measure and log execution time as a metric

Improvements:
_____________
- #1186434551886169: Log pandas histogram calculation time as a metric
- #1183936916078873: Display JSON path as well as content in preview
- #1185884345508633: Add Livy url to external links
- #1183936916078876: Retrieve Qubole traceback and exception when run fails
- #1187615130069532: ci/cd deploy dbnd to quoble/emr from s3 bucket
- #1184859956507691: airflow monitor: print proper error message on RBAC
- #1187144661730497: Added airflow icon to DAG and Operator levels of the graph
- #1187144661730508: Show recent metric values when selected anomaly detection alert
- #1186620728186105: Link from alert list modal to specific run details page

Fixed issues:
_____________
- #1178184224046201: Tracking is slowing down tasks when dbnd-web address is wrong
- #1188853509921970: Add task-version now  to fat_wheel building task
- #1188398439358980: Bring back loading of .dbnd/databand-test.cfg if exist
- #1187836261701377: Fix kubernetes infinite loop on change_state -> delete_pod
- #1188150247653234: support py-files with inline in all engines
- #1189443575273132: Fixed spark and java task graph node icons
- #1186213205378244: Run list table style alignment
- #1187782447252866: Pandas fails to write parquet file to hdfs



DBND v.0.28.01

New features:
____________

- #1184635774415961: Added ability to disable automatic tracking on functions, decorators, and DAGs
- #1186666107312362: Warning message displayed when not all outputs are available for a task.
- #1184186845180735: Support for py_files in the Databricks environment.
- #1185262786942811: Added overwriting of targets via save_options to historically overwrite new data to the same path.
- #1184836053179881: Added ability to configure histograms

Improvements:
_____________
- #1186033707483059: Trackers added to a task/pipeline run banner
- #1185174851651484: Tracking logic moved to dbnd_airflow
- #1183947756269387: Changed the default SparkDataFrameToCsv marshaller behaviour
- #1186029239496897: Added automatic logging for Spark initialization
- #1180558848817017: Automatic configuration of the databand_url on Spark
- Timeline - parent tasks now clearly show when a child task was running
- Show value inline for short params and results instead of the path or a placeholder

Fixed issues:
_____________

- #1186251402357473: --disable-web-tracker function does not work.
- #1183600904070465: 500 HTTP error thrown on bad paramater definition: Format=parameter.choices(["artemis","bago_dialer"]).default("artemis")
- #1186666107312349: Recreated filesystem objects on remote execution to prevent credentials conflict
- Timeline - children didn't always appear below their parents
- Timeline - tasks with the same name were incorrectly grouped together
- Refresh button wasn't working correctly on several pages
- Recent values in alert config showing non-recent values
- fixed run list UI issues
- fixed task sometimes not being selected in the graph during initial page load
- large performance improvements for the stats endpoint exporting metrics to prometheus



DBND v.0.28.00

New features:
–––––––––––––
- Support for fat_wheel for Spark py_files


Improvements:
- New method for canceling pipelines
- Processing tasks all output
- Added ability to move objects across multiple environments
- Logging marshalling operations in DBND
- New pods retry behavior for pipelines running on Kubernetes
