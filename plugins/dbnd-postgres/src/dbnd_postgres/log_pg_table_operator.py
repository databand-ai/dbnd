from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from dbnd._core.tracking.metrics import log_pg_table


class LogPostgresTableOperator(BaseOperator):
    @apply_defaults
    def __init__(self, table_name, conn_id, *args, **kwargs):
        super(LogPostgresTableOperator, self).__init__(
            *args, **kwargs
        )  # py2.7 compatibility
        self.table_name = table_name
        self.conn_id = conn_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        connection_string = hook.get_uri()
        log_pg_table(
            self.table_name, connection_string, with_histograms=True,
        )
