---
"title": "Tracking Azkaban"
---
Databand provides integration with [Azkaban Workflow Manager](https://azkaban.github.io/). Databand will sync flows and jobs, will extract job params, and will connect actual job runs (for instance Spark jobs) to corresponding Azkaban jobs.

## Azkaban version 3.38.2 and below

To enable integration, Databand Agent should be added to Azkaban:

1. Obtain Databand Access Token as described in section [Databand Access Tokens](https://docs.databand.ai/docs/getting-started-with-databand-cloud#databand-access-tokens)

2. Download the Java agent library using the following [Installing JVM DBND Library and Agent](doc:installing-jvm-dbnd#dbnd-jvm-agent).

3. Add the Java agent to the Azkaban startup params:
`-javaagent:/<path-to-agent>/dbnd-azkaban-agent-0.xx.x-all.jar`.

4. Setup Azkaban environment variables: 
``` bash
DBND__CORE__DATABAND_URL=URL_TO_DATABAND_SERVICE
DBND__CORE__DATABAND_ACCESS_TOKEN=PERSONAL_ACCESS_TOKEN
DBND__TRACKING=True
DBND__TRACKING__LOG_VALUE_PREVIEW=True
DBND__LOG__PREVIEW_HEAD_BYTES=1048576
DBND__LOG__PREVIEW_TAIL_BYTES=1048576
```

5. Restart the Azkaban instance so it will run with the Databand Agent enabled. You should see messages like `Starting Databand v0.xx.x for Azkaban!` in the server logs.

### On Update
1. Download the Java agent library using the following [Installing JVM DBND Library and Agent](doc:installing-jvm-dbnd#dbnd-jvm-agent).

2. Update the Java agent version at the Azkaban startup params:
`-javaagent:/<path-to-agent>/dbnd-azkaban-agent-0.xx.x-all.jar`.
 
3. Restart the Azkaban instance so it will run with the Databand Agent enabled. You should see messages like `Starting Databand v0.xx.x for Azkaban!` in the server logs.

[optional] You can remove the old `dbnd-azkaban-agent-0.xx.x-all.jar`

## Azkaban version 3.38.3 and above

To enable integration, Databand Event reporter should be added to Azkaban

1. Download the Java agent library using the following [Installing JVM DBND Library and Agent](doc:installing-jvm-dbnd#dbnd-jvm-agent).

2. Add Event Reporter to Azkaban properties file:
`azkaban.event.reporting.enabled=true`
`azkaban.event.reporting.class=ai.databand.azkaban.DbndEventReporter`.

3. Add `dbnd-azkaban-event-reporter-0.xx.x-all.jar` to Azkaban jar dirs.

4. Set up Databand url as mentioned in section "Step 4: Set Up the Databand Tracker URL and Configure Tracking"


## Azkaban Tracking Configuration 
Following configuration properties are supported as a part of Azkaban integration. These properties should be set on Azkaban instance and can not be passed as a part of Spark job.

| Variable | Description |
|---|---|
| `DBND__AZKABAN__SYNC_PROJECTS` | List of Azkaban projects to sync. If not specified, all projects will be synced. |
| `DBND__AZKABAN__SYNC_FLOWS` | List of Azkaban flows to sync. If not specified, all flows will be synced. |
[block:html]
{
  "html": "<style>\n  pre {\n      border: 0.2px solid #ddd;\n      border-left: 3px solid #c796ff;\n      color: #0061a6;\n  }\n\n.CodeTabs_initial{\n  /* box shadows with with legacy browser support - just in case */\n    -webkit-box-shadow: 0 10px 6px -6px #777; /* for Safari 3-4, iOS 4.0.2 - 4.2, Android 2.3+ */\n     -moz-box-shadow: 0 10px 6px -6px #777; /* for Firefox 3.5 - 3.6 */\n          box-shadow: 0 10px 6px -6px #777;/* Opera 10.5, IE 9, Firefox 4+, Chrome 6+, iOS 5 */\n  }\n</style>"
}
[/block]