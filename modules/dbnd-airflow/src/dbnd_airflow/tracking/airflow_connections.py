from targets.connections import build_conn_path


def get_conn_path(conn_id):
    from airflow.hooks.base_hook import BaseHook
    from airflow.models import Connection

    connection = BaseHook.get_connection(conn_id)  # type: Connection
    return build_conn_path(
        conn_type=connection.conn_type,
        hostname=connection.host,
        port=connection.port,
        path=connection.schema,
    )
