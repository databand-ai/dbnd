from pandas import DataFrame

from dbnd import Task, parameter


class BigQuery(Task):
    query = parameter[str]
    output = parameter.output[DataFrame]

    def run(self):
        from airflow.contrib.hooks.bigquery_hook import BigQueryHook

        hook = BigQueryHook(bigquery_conn_id="bigquery_default", use_legacy_sql=False)
        self.output = hook.get_pandas_df(self.query)
