# Databand Airflow Monitor

Databand Airflow Monitor is a stand-alone module for Databand system, enables you to load data from Airflow server and import it into Databand system.
This Databand side module is one of two components allows you to sync your Airflow data into Databand system.

## Installation with setup tools

```bash
cd modules/dbnd-airflow-monitor
pip install -e .
```

## Usage

`dbnd airflow-monitor`

### Important flags

`--sync-history`: by default, airflow monitor's `since` value will be determined by last time it was running. use this flag to enable syncning from beginning

### Configuration

You can configure your syncing variables inside databand configuration system

```cfg
[airflow_monitor]
interval = 10 ; Time in seconds to wait between each fetching cycle
include_logs = True ; Whether or not to include logs (might be heavy)
include_task_args = True ; Whether or not to include task arguments
fetch_quantity = 100 ; Max number of tasks or dag runs to retrieve at each fetch
fetch_period = 60 ; Time in minutes for window fetching size (start: since, end: since + period)
dag_ids = ['ingest_data_dag', 'simple_dag'] ; Limit fetching to these specific dag ids

## DB Fetcher
### Pay attention, when using this system airflow version must be equal to databand's airflow version
sql_alchemy_conn = sqlite:////usr/local/airflow/airflow.db ; When using fetcher=db, use this sql connection string
local_dag_folder =  /usr/local/airflow/dags ; When using fetcher=db, this is the dag folder location
```

## Steps for Google Composer

​
After spinning new google composer, under PyPi packages add dbnd, and add `DBND__CORE__DATABAND_URL` env pointing to dnbd instance, copy plugin file to pluings folder (go to dags folder, one level up, and then plugins)
​
​
For monitor to work you will need to setup service account (add relevant binding):
(taken from here: https://medium.com/google-cloud/using-airflow-experimental-rest-api-on-google-cloud-platform-cloud-composer-and-iap-9bd0260f095a
see Create a Service Account for POST Trigger section)
​
example with creating new SA:

```bash
export PROJECT=prefab-root-227507
export SERVICE_ACCOUNT_NAME=dbnd-airflow-monitor
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME --project $PROJECT
# Give service account permissions to create tokens for iap requests.
gcloud projects add-iam-policy-binding $PROJECT --member serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com --role roles/iam.serviceAccountTokenCreator
gcloud projects add-iam-policy-binding $PROJECT --member serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com --role roles/iam.serviceAccountActor
# Service account also needs to be authorized to use Composer.
gcloud projects add-iam-policy-binding $PROJECT --member serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com --role roles/composer.user
# We need a service account key to trigger the dag.
gcloud iam service-accounts keys create ~/$PROJECT-$SERVICE_ACCOUNT_NAME-key.json --iam-account=$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com
export GOOGLE_APPLICATION_CREDENTIALS=~/$PROJECT-$SERVICE_ACCOUNT_NAME-key.json
```

​
configure airflow monitor with composer fetcher, with url pointing to composer airflow instance and client id (same article, Getting Airflow Client ID section):
Visit the Airflow URL https://YOUR_UNIQUE_ID.appspot.com (which you noted in the last step) in an incognito window, don’t login. At this first landing page for IAP Auth has client id in the url in the address bar:

```
https://accounts.google.com/signin/oauth/identifier?client_id=00000000000-xxxx0x0xx0xx00xxxx0x00xxx0xxxxx.apps.googleusercontent.com&...
```

## Integration Tests

We have 2 tests:

-   databand/integration-tests/airflow_monitor
-   databand/integration-tests/airflow_monitor_stress

To run them, go to the right dir and run inttest container:

```
cd databand/integration-tests/airflow_monitor
docker-compose up inttest
```
