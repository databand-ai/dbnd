History
=======


DBND v.0.28.01

New features:
____________

- #1184635774415961: Added ability to disable automatic tracking on functions, decorators, and DAGs
- #1186666107312362: Warning message displayed when not all outputs are available for a task.
- #1184186845180735: Support for py_files in the Databricks environment.
- #1185262786942811: Added overwriting of targets via save_options to historically overwrite new data to the same path.

Improvements:
_____________
- #1184836053179881: Refactored log_dataframe() histograms
- #1186033707483059: Trackers added to a task/pipeline run banner
- #1185174851651484: Tracking logic moved to dbnd_airflow
- #1183947756269387: Changed the default SparkDataFrameToCsv marshaller behaviour
- #1186029239496897: Added automatic logging for Spark initialization
- #1180558848817017: Automatic configuration of the databand_url on Spark

Fixed issues:
_____________

- #1186251402357473: --disable-web-tracker function does not work.
- #1183600904070465: 500 HTTP error thrown on bad paramater definition: Format=parameter.choices(["artemis","bago_dialer"]).default("artemis")
- #1186666107312349: Recreated filesystem objects on remote execution to prevent credentials conflict



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