# History

## DBND v0.43.00

### New features

-   \#1200445773961849: Introducing `log_dataset_op` - now you can manually log the dataset you are interacting with.
-   \#1200130408884030: Outside-of-the-box tracking support for PySpark with Databricks.
-   \#1199370081934662: Added Live Logs for Kubernetes workers in the Airflow UI.
-   \#1200401140575119: Missing Data now triggers an alert.
-   \#1200454655158822: Now you can calculate metric aggregations like SUM/AVG and others and display them in the Favorite charts widget. You can also compare trends over the filtered time period and display trends.
-   \#1200430623687636: Dataset Screen is deprecated and replaced by Data Targets and Datasets screen in the UI.
-   \#1200466625626565: Added support for custom headers for Alert Manager webhooks.

### Improvements

-   \#1200403795815183: Improved Favorite Metric widget usability.
-   \#1200463339266254: Improved Metric tab interface.
-   \#1200452512156534: ‘Triggered by column’ is removed from the Run list.
-   \#1200408168252754: History tab is removed from the Run details.
-   \#1200430599620006: Timeline Tab in the Run details is deprecated.
-   \#1200181993522176: Proper support for DagRun submit args and XCom; values are reported with correct types.
-   \#1200275295497774: Added support for the Airflow retries and restarts in Alerts.
-   \#1200372371285197: It’s now possible to expose Run env and user in the `stats_api` for alerting.

### Bug fixes

-   \#1200402990123287: Fixed a bug that prevented proper tracking from Databricks.
-   \#1200358546721956: Metrics that cannot be presented in a graph now cannot be added.
-   \#1200275951379648: Fixed incorrect handling of attempts.
-   \#1200440061411916: Fixed tracking tasks from `@task`.
-   \#1200035644894886: Fixed browser ‘Back’ button behavior in the Run details page.
-   \#1200503204031963: Fixed bug that prevented to define advanced alert

## DBND v0.42.00

### New features

-   \#1200378732521423: Airflow Monitor v2 now supports Airflow 2.0.
-   \#1200392175180332: The Favorite Metric widget in the Dashboard now automatically displays anomalies for metrics.
-   \#1200213466288282: Now supporting Google Composer for Airflow tracking.
-   \#1200340430085787: Main menu now expands to include titles.
-   \#1200321463111129: Added a chart display toggle for switching between Run charts in the Favorite Metric widget.
-   \#1200095672805698: Users can select the main page view. The default mode is Dashboard; options: Dashboard or Pipelines.
-   \#1200309661632765: The Syncer page was moved to the Settings page.
-   \#1200284286454068: Now you can identify errors in different Runs - both the error message and the error type now lead to the Run page; the task that triggered the error is selected so you can view the error.
    ​

### Improvements

-   \#1200403425760171: Added new Syncer issue counters in the Settings section.
-   \#1200420269065898: Only the table is now scrollable in the Metrics widget.
-   \#1200400792119627: Changes in the chart type design.
-   \#1200400792119629: Changes in the Dashboard chart tooltips - now, the nearest point tooltip will be displayed.
-   \#1200251794221201: Improved the readability of the metric charts tooltip.
-   \#1200378661150501: Added Error Column to All Runs and Pipeline Runs lists.
-   \#1200369692119674: Improved usability of the Alert Definition section.
-   \#1200374990956464: Tasks in Airflow sub-processes are now automatically assigned unique names.
-   \#1200102937873725: Added a unified context manager for handling log dataset operations.
-   \#1200279378863216: Aligned and improved alert counters in the Pipeline and Run lists.
    ​

### Bug fixes

-   \#1200401407223048: Fixed custom date picker behavior and placed it next to the Pipeline filter.
-   \#1200126969897499: Task ID is now correctly displayed in the UI.

## DBND v0.41.00

### New features

-   \#1200253837838312: Added Deequ + PySpark integration.
-   \#1200245988366766: Added multiple metrics support in the Favorite Metric widget.
-   \#1200095672805698: Home Screen selection option is now available in the Settings Page.
-   \#1200306047670389: It’s now possible to enable the debug mode in PyDev from the CLI.
-   \#1200179220565213: Slack notifications can now be received from the Databand UI.

### Improvements

-   \#1199965369989833: Tracking is now compatible with Airflow 2.0.
-   \#1200213466288282: Airflow tracking is running on Google Composer.
-   \#1200105822969991: Added support for liveness probe check in Airflow Monitor v2.0.
-   \#1200327639593933: Latest Pandas version (1.2.x) is now supported.
-   \#1200223284217045: Improved Alert screen.
-   \#1200345876827058: Alert rules can be enabled based on the Run env.
-   \#1200269646547649: “Last Active” widget moved to the bottom of the screen.
-   \#1200269646547644: Pipeline screen's default view is set to “List”.
-   \#1200310831229608: PyWheel build tasks are not displayed in the UI now.
-   \#1200315872631815: Changed the cursor in Favorite Charts.

### Bug fixes

-   \#1200305391366264: Fixed missing filters and Run names in the Alert API.
-   \#1200324498682162: Identified and fixed the issue with Sentry integration.
-   \#1200275951379648: Fixed incorrect handling of attempts.
-   \#1200290917819476: Changed Error Widget headline to “Top Errors”.
-   \#1200359124635729: Fixed error on wrong outputs’ schema in Orchestration.

## DBND v0.40.10

### New features

-   \#1200183306770565: Stats widget changed to display the top failure rate Pipelines.
-   \#1200191541441378: Server-side sorting for Pipeline stats widget.
-   \#1200258224502988: New Anomaly Detection alert is triggered if Custom Metric grows too fast.
-   \#1200259328525645: Airflow Monitor v.2.
-   \#1200183150156169: It’s now possible to see an alert’s effect on the affected data.

### Improvements

-   \#1200044288700180: Drill down to Runs & Pipelines from the Dashboard.
-   \#1200259770331112: Click iteration and reporting stats added to Dashboard.
-   \#1200058439151159: Airflow monitor heartbeat indication.
-   \#1200243606518228: Errors Discovery Flow added for non-Python Operators.

### Bug fixes

-   \#1200276060368190: Negative values are now displayed in Dashboard charts.
-   \#1200285224722668: Fixed Docker version incompatibility issue (extended the number of supported versions).
-   \#1200275951379659: Fixed Kubernetes execution bug when tasks were wrongly marked as canceled on the end of Pipelines.

## DBND v0.39.00

### New features

-   \#1168991558489592: Auto-log paths became available for Spark JVM I/O (Alpha version).
-   \#1200183406021457: It’s now possible to trigger a numeric alert when an anomaly is detected.
-   \#1200182230484446: Added alerts integration with Opsgenie.

### Improvements

-   \#1199534439165430: Tracking system error handling is out - enjoy safe tracking without the performance penalty.
-   \#1200183406021460: See how a newly defined alert would have affected your past runs.
-   \#1200183406021459: Define sensitivity and the number of runs for anomaly detection.
-   \#1200194910187394: Slack alert headline now leads to the Alert page and displays the number of firing alerts.

### Bug fixes

-   \#1200141500926246: Fixed the URL in Slack message alerts.
-   \#1200194910187389: Slack alert’s link is now leading to the first alert.

## DBND v0.38.00

### New features

-   \#1199905812865631: Added Favorite Metrics widget.
-   \#1199905812865626: Added reference date and fix time range widget.
-   \#1200036364804040: Added support for Date picker in the UI.
-   \#1199908347034091: Added Python Spark Metrics listener (similar to JVM).
-   \#1200135620545740: New alerting mechanism for on-prem users (available on demand).
-   \#1200130408884041: Zero-cost integration of SQL Airflow DAGs with Snowflake (in DBND SDK).

### Improvements

-   \#1199510429341979: Configured POC alerting.
-   \#1199592642123487: Run name/run description is now editable from the UI.
-   \#1199917986044490: It’s now possible to access the latest webserver logs via the webserver.
-   \#1200092086191209: Added a descriptive error message that appears when installing Airflow Monitor not in the same folder as Airflow.

## DBND v0.37.2

### New features

-   \#1200030168701007: Support for the cloud UI is here. Now you can gain access to the Databand UI in the cloud.
-   \#1200016204502957: Users who have Airflow with Docker Compose environment can now use the Databand cloud solution.
-   \#1200058439151184: A flag for switching on\off the transmission (sending) of the source code was added.

### Improvements

-   \#1199921442266004: AlertDef history now displays seconds in the UI.
-   \#1199995565427900: Version and Environment badges are now displayed on the Login page.
-   \#1200093251774759: Graphic info card in the Dashboard is now black.
-   \#1200127804370172: Quick updates tab added to the integration page.
-   \#1199657019529688: `QueryApi.force_pagination` is now enabled by default.
-   \#1200126969897487: Remote logs are truncated.
-   \#1200058448826966: Support for "zero computational cost" data insights and preview added.

### Bug fixes

-   \#1200129802903561: Databand logo back on the Login page.
-   \#1200111167427102: Time format bug is fixed.
-   \#1199965226804188: The search bar is back in Details view.
-   \#1200091724058557: Runs and Task by Start Time are a stacked graph.

## DBND v0.36.00

### New features:

-   \#1199594880117673: Introducing the Cloud Environment of Databand Web server.
-   \#1199643165776154: New Design and filters for Run List, including sticky filters.
-   \#1199914881669553: Project filters now support Airflow tags.
-   \#1199914881669552: Azkaban projects are now supported.
-   \#1199371447621207: You can now track Log paths from Spark.
-   \#1199674986608158: Extended tracking of Operational Health in the Dashboard: see Runs and Task Runs over time.

### Improvements:

-   \#1199965687445132: Added columns for the task table in the Dashboard.
-   \#1199917986044517: Added Spark LogPath.

## DBND v0.35.00

### New features:

-   \#1199534085669848: Orchestration error Reporting
-   \#1199382295296733: Project phase 1
-   \#1199562651279654: Support multiple Azkaban instances

## DBND v0.34.00

### New features:

-   \#1199503238625634: Support user access token

### Improvements:

-   \#1199410590823866: Using pipeline(=band) outputs in runtime

### Fixed issues:

-   \#1199512537243676: Enable pod logs collection with google logs
-   \#1199511121032309: Zombie Driver at GKE - unsynced state
-   \#1199538657255959: Link at Error banner at Details page looks broken
-   \#1199624982127115: Configuration per task run and not all pipeline run
-   \#1199560164027872: Zombie Drive at GKE - watcher process not dying on driver termination
-   \#1199665645625989: fix Marshalling of nested lists
-   \#1199692629063013: log_dataframe(...,with_stats=True) does not print stats to CLI

## DBND v0.33.00

### New features:

-   \#1199539989855853: silence logging on tracking in production
-   \#1199410183946263: [java sdk] log collection - bring both head and tail
-   \#1186815542031177: add @data_source_pipeline annotation

### Improvements:

-   \#1199601902803965: Default delay is set to 1 for log_snowflake_resource_usage
-   \#1183486068581313: auto-add dbnd spark listener
-   \#1195941695830497: Clean console outputs in tracking scenario

### Fixed issues:

-   \#1199534865809603: fix DbndSchedulerDBDagsProvider retries
-   \#1199553897458772: Pendulum fix for log_metric
-   \#1199592885591350: EKS(AWS) submitter log read timeout after 4h
-   \#1199383391833788: Normalize

## DBND v0.32.00

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

## DBND v0.31.00

### Improvements:

-   \#1199351713607668: log collection - bring both head and tail
-   \#1199222341211949: make log_metric() compatible with BIGINTs
-   \#1199191513095141: remove git working from log
-   \#1199352826744440: Better run name and job name for Azkaban

### Fixed issues:

-   \#1185047487019177: add dataframe name to target reporting with log_dataframe()
-   \#1198956007770574: fix jvm tracking timeouts

## DBND v0.30.04

### Fixed Issues:

-   \#1196324185480705: make databand task return proper result
-   \#1198935333953725: fix pickling compatibility issue for fat wheel.

## DBND v0.30.00

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

## DBND v0.29.00

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

## DBND v0.28.26

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

## DBND v0.28.20

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

## DBND v0.28.13

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

## DBND v0.28.6

### Improvements:

-   \#1171185232799219: support --set run.task
-   \#1184271040807809: Parallelize checking if tasks are complete
-   \#1189508556532473: support airflow 1.10.10
-   \#1183936916078880: Retrieve spark exception when running spark locally

## DBND v0.28.05

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

## DBND v0.28.01

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

## DBND v0.28.00

### New features:

-   Support for fat_wheel for Spark py_files

### Improvements:

-   New method for canceling pipelines
-   Processing tasks all output
-   Added ability to move objects across multiple environments
-   Logging marshalling operations in DBND
-   New pods retry behavior for pipelines running on Kubernetes
