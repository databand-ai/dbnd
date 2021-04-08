def get_conn_path(conn_id):
    from airflow.hooks.base_hook import BaseHook
    from airflow.models import Connection

    connection = BaseHook.get_connection(conn_id)  # type: Connection

    return "{type}://{hostname}{port}{database}".format(
        type=connection.conn_type,
        hostname=connection.host if connection.host else "",
        port=":" + str(connection.port) if connection.port else "",
        database=connection.schema.rstrip("/"),
    )
