import mock

from dbnd_redshift.dbnd_redshift import (
    get_redshift_query_id,
    log_redshift_resource_usage,
)


def redshift_snippet(redshift_conn):
    query_id = get_redshift_query_id(
        "select * from my_table limit 20", connection_string=redshift_conn
    )
    log_redshift_resource_usage(query_id, connection_string=redshift_conn)


class TestDbTracking(object):
    def test_redshift_code(self):
        with mock.patch("psycopg2.connect") as redshift_conn:
            redshift_snippet(redshift_conn)
