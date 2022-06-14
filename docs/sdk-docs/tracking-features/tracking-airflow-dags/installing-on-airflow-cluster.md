---
"title": "Installing on Airflow Cluster"
---
Databand integrates with different Apache Airflow deployment types to provide observability over your Airflow DAGs. This guide will cover platform-specific steps for tracking Apache Airflow in Databand. After following the platform-specific steps below, you will add our Airflow monitor DAG to your cluster and create an Airflow syncer in your Databand UI.

# Platform-Specific Steps
For the steps specific to your type of Airflow deployment, use the links below to jump to the relevant section:
* [Integrating with Standard Apache Airflow Cluster](doc:installing-on-airflow-cluster#standard-apache-airflow-cluster)
* [Integrating Amazon Managed Workflows Airflow](doc:installing-on-airflow-cluster#aws-managed-workflows)
* [Integrating Google Cloud Composer](doc:installing-on-airflow-cluster#google-cloud-composer)
* [Integrating Astronomer](doc:installing-on-airflow-cluster#astronomer)

| Make sure you have network connectivity between Apache Airflow and Databand (from Apache Airflow to the Databand Server)

## Standard Apache Airflow Cluster
Follow our standard [Installing Python SDK](doc:installing-dbnd)  manual to install Databand's  `dbnd-airflow-auto-tracking` Python package into your cluster's Python environment. You might have to change your Dockerfile or requirements.txt if you have one.

*NOTE: Installing new Python packages on managed Airflow environments will trigger an automatic restart of the Airflow scheduler.*


### Airflow 2.0+ support
For Databand tracking to work properly with Airflow 2.0+, you need to disable lazily loaded plugins. This can be done using the `core.lazy_load_plugins=False` configuration setting or by setting the environment variable `AIRFLOW__CORE__LAZY_LOAD_PLUGINS=False`.

You can read more about lazily loaded plugins in the [Airflow's plugins documentation](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html#when-are-plugins-re-loaded).

### Airflow 2.1.[0-2] - Airflow HTTP communication
If you are using Airflow version 2.1.0, 2.1.1, or 2.1.2, please verify that the `apache-airflow-providers-http` package is installed, or consider upgrading your Airflow.


## Astronomer

[Astronomer](https://www.astronomer.io/) allows you to build, run, and manage data pipelines-as-code at enterprise scale.

You can install dbnd by customizing the Astronomer Docker image, rebuilding it, and deploying it.
Check this [manual](https://docs.astronomer.io/enterprise/customize-image/) for more details on how to do that.

In your Astronomer folder, add the following line to your `requirements.txt` file:
```
dbnd-airflow-auto-tracking==REPLACE_WITH_DATABAND_VERSION
```

*NOTE: Re-deploying the Airflow image will trigger a restart of your Airflow scheduler.*

### Astronomer Airflow URL
You can find your Airflow URL by going to the Astronomer control panel and selecting the Airflow deployment. There you can click on “Open Airflow” and copy the URL without the “/home” suffix.

The URL should be in the following format:
`http://deployments.{your_domain}.com/{deployment-name}/airflow`

Your {deployment-name} should be shown in the Astronomer UI as "Release Name".

![Screen Shot 2022-02-09 at 9.33.00.png](https://files.readme.io/4832728-Screen_Shot_2022-02-09_at_9.33.00.png)

When creating a Databand Airflow syncer for Airflow deployed on Astronomer, select 'OnPrem Airflow' as the Airflow mode, and enter the Airflow URL from above in the Airflow URL field.


## AWS Managed Workflows

[Amazon Managed Workflows](https://aws.amazon.com/managed-workflows-for-apache-airflow/getting-started/) is a managed Apache Airflow service that makes it easier to set up and operate end-to-end data pipelines in the AWS cloud at scale.

Go to *AWS MWAA* **>** *Environments* **>** *{mwaa_env_name}* **>** *DAG code in Amazon S3* **>** *S3 Bucket*:

![MWAA-2.png](https://files.readme.io/4a5fd09-MWAA-2.png)

In MWAA’s S3 bucket, update your `requirements.txt` file with the following line:
```
dbnd-airflow-auto-tracking==REPLACE_WITH_DATABAND_VERSION
```

Update the `requirements.txt` version in the MWAA environment configuration. **Please note that saving this change to your MWAA environment configuration will trigger a restart of your Airflow scheduler.**

For more information on installing 'extras' in MWAA see [Installing Python dependencies - Amazon Managed Workflows for Apache Airflow](https://docs.aws.amazon.com/mwaa/latest/userguide/working-dags-dependencies.html). For Databand installation details, please check [Installing DBND](doc:installing-dbnd)

### MWAA URL
The Airflow URL can be located in the [AWS Console](http://console.aws.amazon.com/):
Go to *AWS MWAA* **>** *Environments* **>** *{mwaa_env_name}* ** >** *Details* **>** *Airflow UI*.

The URL should be in the following format:
`https://<guid>.<aws_region>.airflow.amazonaws.com`

![MWAA-01.png](https://files.readme.io/00a4835-MWAA-01.png)




##  Google Cloud Composer

[Google Cloud Composer](https://cloud.google.com/composer) is a fully managed data workflow orchestration service that empowers you to author, schedule, and monitor pipelines.

Update your Cloud Composer environment's PyPI packages with the entry below. Use the most recent Databand version (i.e. if you're running version `0.61.1`, this is what you should use instead of `REPLACE_WITH_DBND_VERSION`). See [Installing DBND](doc:installing-dbnd)  for more details:

```
dbnd-airflow-auto-tracking==REPLACE_WITH_DBND_VERSION
```
**Please note that saving this change to your Cloud Composer environment configuration will trigger a restart of your Airflow scheduler!**

![GCC-004.png](https://files.readme.io/917aa8e-GCC-004.png)

For more information on installing packages in Google Cloud Composer, please see [Installing a Python dependency from PyPI](https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies#install-package).

For Databand tracking to work properly with Airflow 2.0+, you need to disable lazily loaded plugins. This can be done using the following configuration setting: [`core.lazy_load_plugins=False`](doc:integrating-databand-with-airflow#airflow-2-support).

The screenshot below provides an example of setting this property in Cloud Composer.

![lazy-load-plugins-disable.png](https://files.readme.io/3a68b8c-lazy-load-plugins-disable.png)



### Cloud Composer URL
Before integrating Cloud Composer with Databand, you will need your Cloud Composer URL.

The URL can be found in the [GCloud Console](https://console.cloud.google.com/):
*Composer* **>** *{composer_env_name}* **>** *Environment Configuration* > *Airflow web UI*.

The URL should be in the following format:
`https://<guid>.appspot.com`


# Installing Airflow Syncer DAG
1. Create a new DAG named `databand_airflow_monitor.py` with the following DAG definition, and add it to your project's DAGs folder:

<!-- noqa -->
``` python
from airflow_monitor.monitor_as_dag import get_monitor_dag
# This DAG is used by Databand to monitor your Airflow installation.
dag = get_monitor_dag()
```
2. Deploy your new DAG, and *enable* it in the Airflow UI.

*NOTE: The monitor dag is being run automatically by the scheduler. There can be only one run at a given time, there is no need to trigger it manually. It is configured to restart itself every 3 hours. In case it's not running or failed for some reason, there is no lose of data, since when it's running, it will continue syncing data from where it stopped.*

# Enable Airflow Syncer in Databand Service
To complete the configuration, create an Airflow syncer in Databand, and create an Airflow connection with your Databand URL and configuration parameters in your Airflow deployment. See [Airflow Syncer Configuration](doc:apache-airflow-sync)  for detailed instructions.
1. Click on "Integrations" at the left-side menu of Databand UI.
2. Click on "Add Syncer".
