---
"title": "Tracking DataStage"
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

3. Choose whether you are an on-prem user or a cloud user in the following popup: 

    ![datastage user type](https://files.readme.io/13d0431-datastage_user_type.png)

4. Follow the additional steps in the section below that is relevant to your user type:

### On-prem users
5. In the syncer configuration, provide the following details, and then proceed to [step 6](#step_6):   

    ![add datastage on-prem syncer](https://files.readme.io/dda270f-datastage_onprem_config.png)

    * **Syncer name** - This will become the name of your DataStage syncer in the Databand UI.

    * **Hostname** - The URL you use to access your on-prem installation of DataStage. 
    
    **Select authentication method**:
    * **Basic authentication**

        * **Username/Password** - The credentials you use to access your on-prem installation of DataStage. 
    
    **Advanced settings**:
     * **Number of threads** - The number of concurrent threads to use on the DataStage API client. The default recommended value is 2.

### Cloud users
5. In the syncer configuration, provide the following details, and then proceed to [step 6](#step_6): 

    ![add datastage cloud syncer](https://files.readme.io/63d4d74-datastage_cloud_config.png)

    * **Syncer name** - This will become the name of your DataStage syncer in the Databand UI.

    * **API key** - The API key will allow Databand to authenticate with your DataStage project. To generate a new API key for your user identity, follow [these steps](https://cloud.ibm.com/docs/account?topic=account-userapikey&interface=ui#create_user_key) outlined in the IBM documentation.
    
    **Advanced Settings**:
     * **Number of threads** - The number of concurrent threads to use on the DataStage API client. The default recommended value is 2.

### All users
6. After providing the required parameters, click **Next**.  <a name="step_6"></a>
7. You will be taken to a list of all DataStage projects that are accessible based on the authentication method previously provided. This will include any projects you've created yourself as well as any projects that have been shared with you. Select the projects that you would like for the syncer to monitor, and then click **Save**.

    ![datastage projects list](https://files.readme.io/af5f5ee-datastage_projects_list.png)

    ***NOTE***: *A DataStage project can only be monitored by a single syncer in Databand. If any of the projects to which you have access have already been added to another Databand syncer, those projects will be grayed out, and you will be unable to add them to your syncer.*

8. (**Optional**) By default, the source name attributed to a DataStage project and all of its jobs will be the account ID under which that project is located. If you would like to change the source name, click the edit button next to the account ID, enter a new source name, and then click the check mark to save it. 

    ![datastage source name](https://files.readme.io/7b8ada2-datastage_source_name.png)

    ***NOTE***: *When you modify the source name for a particular account ID, that source name will apply to all instances of that account ID throughout Databand. This means that the source name for any existing pipelines monitored under that account ID will also change, and any future pipelines that may be monitored under that account ID will also reflect the source name you have specified.*

Once these steps have been completed, the next time a job runs in your DataStage project, you will see it in your Databand UI. The name of your job in the DataStage UI will become the pipeline name in the Databand UI.

# Editing an Existing DataStage Syncer

1. Click on **Settings** in the lefthand menu.
2. Click on **Datasource Syncers** in the settings menu.
3. Click the button in the **Actions** column, and select either **Edit Syncer** or **Edit List of Projects** from the context menu.
4. Make the necessary changes in your configuration, and then click the **Save** button.

![edit datastage syncer](https://files.readme.io/1606f6e-editing_datastage_syncer.png)


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
1. Global subflows and custom steps are not yet supported.