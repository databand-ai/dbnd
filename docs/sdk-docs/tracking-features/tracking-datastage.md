---
"title": "Tracking Datastage"
---

# Overview
With Databand, you can track the execution of your DataStage jobs. This is achieved through the use of a syncer that will scan your DataStage project every few seconds and report on collected metadata from any jobs that have been run. With the metadata collected, you can enable powerful alerting to notify your data team on the health of your jobs and the quality of your inputs and outputs. 

# Available Alerting
Databand natively offers the following alerts for your DataStage job runs:
- Run and task state alerts (e.g. running, successful, failed, etc.)
- Run duration alerts
  - anomaly detection
  - percent ranges (e.g. duration within 20% of 100 seconds)
  - basic comparison operators (e.g. duration > 100 seconds)
- Schema changes for inputs and outputs
  - new columns added
  - old columns removed
  - datatypes of existing columns changed
- Record counts for inputs and outputs
  - anomaly detection
  - percent ranges (e.g. record count within 20% of 100,000 rows)
  - basic comparison operators (e.g. record count > 100,000 rows)


# Creating a DataStage Syncer:

To begin monitoring your DataStage project in Databand, start by creating a DataStage syncer in the Databand UI:

1. Click on **Integrations** in the lefthand menu.
2. Click the **Connect** button under DataStage.

    ![datastage syncer](https://files.readme.io/5cccdd1-datastage_syncer.png)

3. In the syncer configuration, provide the following details:  

    ![add datastage syncer](https://files.readme.io/a427ad7-datastage_configuration.png)


      * **Source name** - This will become the name of your DataStage syncer in the Databand UI and will allow you to filter flows based on their DataStage projects.
      * **Project ID** - The ID of the DataStage project you would like to monitor. The project ID can be found in the URL of your DataStage project.

        ![project id](https://files.readme.io/f08d634-datastage_project_id.png)

      * **API key** - The API key will allow Databand to authenticate with your DataStage project. To generate a new API key for your user identity, follow [these steps](https://cloud.ibm.com/docs/account?topic=account-userapikey&interface=ui#create_user_key) outlined in the IBM documentation.
    
    **Advanced Settings**:

     * **Hostname** - The hostname for an on-prem deployment of DataStage.
     * **IAM Service URL** - The hostname for an on-prem IAM authentication service of DataStage.
     * **Number of threads** - The number of concurrent threads to use on the DataStage API client. The default recommended value is 2.

4. After providing the required parameters, click **Save**.

Once these steps have been completed, the next time a job runs in your DataStage project, you will see it in your Databand UI. The name of your job in the DataStage UI will become the pipeline name in the Databand UI.

# Editing an Existing DataStage Syncer

1. Click on **Settings** in the lefthand menu.
2. Click on **Datasource Syncers** in the settings menu.
3. Click the button in the **Actions** column, and select **Edit** from the context menu.
4. Make the necessary changes in the syncer configuration, and then click the **Save** button.

![edit datastage syncer](https://files.readme.io/1ec6d01-edit_datastage_syncer.png)


# Metadata Collected

Databand will collect high level information about the execution of your DataStage jobs as well as general information about the inputs and outputs of your stages. The metadata collected includes the following:

### Graphical representation of the flow
![a flow in datastage](https://files.readme.io/fd36835-datastage_flow.png)
<div style="text-align: center;"><em>A flow in DataStage</em></div>

![the same flow in databand](https://files.readme.io/1158a66-datastage_flow_in_databand.png)
<div style="text-align: center;"><em>The same flow in Databand</em></div>

### Logs from each stage
![datastage logs](https://files.readme.io/9a35f2f-datastage_logs.png)

### Start and end times, duration, and run ID of the job execution
![datastage run info](https://files.readme.io/8a9a2fe-datastage_run_info.png)

### Job execution and dataset metrics
![datastage metrics](https://files.readme.io/d2c9630-datastage_metrics.png)

### Inputs and outputs of the job
![datastage inputs](https://files.readme.io/8bda26b-datastage_inputs_outputs.png)

# Current Limitations
1. Each syncer only supports a single DataStage project. Soon, users will be able to add multiple projects to a single syncer.
2. Subflows and custom steps are not yet supported.