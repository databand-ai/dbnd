---
"title": "Tracking BigQuery"
---
# Using BigQuery with Databand
Databand's BigQuery Monitor continuously monitors your BigQuery data warehouse. After connecting to BigQuery, Databand will perform metadata collection every 6 hours. 

### Datasets (BigQuery tables)
In Databand, we call BigQuery tables `datasets’. 
The following metadata is tracked over time:
* table size
* row count
* schema
* last update time
* creation date

### Transactions (BigQuery jobs)
In Databand, *BigQuery jobs* are called transactions (see more [here](https://cloud.google.com/bigquery/docs/jobs-overview)).  Jobs are actions that BigQuery runs on your behalf to load data, export data, query data, or copy data.

The following metadata is saved and exposed on transactions: query, create date, start date, end date, and operations. Databand analyzes the query plan to determine the different operations (read/ write) performed on different datasets and saves the record count of each operation.
 
After connecting to BigQuery, Databand will monitor the metadata from the last 12 hours and will perform metadata collection every 6 hours thereafter. Every time we collect all the datasets (tables) snapshots: schema, size in records, size in bytes.

>Note: Currently, only the BigQuery QueryJob type can be tracked using Databand. CopyJob, LoadJob, and ExtractJob are not supported yet. 


Please see [BigQuery](doc:bigquery) for the BigQuery Monitor setup instructions.