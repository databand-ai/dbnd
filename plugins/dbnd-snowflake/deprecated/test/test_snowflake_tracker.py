# © Copyright Databand.ai, an IBM Company 2022

import random

import pytest
import snowflake

from mock import Mock, call, patch

from dbnd.testing.helpers_mocks import set_airflow_context
from dbnd_snowflake.snowflake_controller import SNOWFLAKE_METRIC_TO_UI_NAME
from dbnd_snowflake.snowflake_tracker import snowflake_query_tracker
from dbnd_snowflake.sql_utils import try_extract_tables


TEST_SNOWFLAKE_CONN_STRING = (
    "snowflake://test_account:test_password@test_account/test_database"
)

TEST_TABLE_NAME = "some_table"


def _authenticate(self_auth, *args, **kwargs):
    self_auth._rest._connection._session_id = random.randint(0, 2000000000)
    return {}


@pytest.fixture
def mock_snowflake():
    execute_mock = Mock()

    def _execute(self_cursor, command, *args, **kwargs):
        # type: (snowflake.connector.cursor.SnowflakeCursor, str, ..., ...) -> ...
        execute_mock(command, *args, **kwargs)
        self_cursor._sfqid = random.randint(0, 2000000000)

        some_key = list(SNOWFLAKE_METRIC_TO_UI_NAME)[0]
        if some_key in command:
            # this probably Resource Usage query, generate dummy metric
            result = [{k: 0 for k in SNOWFLAKE_METRIC_TO_UI_NAME}]
        elif "information_schema.columns" in command:
            # probably, SnowflakeController.get_column_types()
            result = [
                {"COLUMN_NAME": "column_a", "DATA_TYPE": "int"},
                {"COLUMN_NAME": "column_b", "DATA_TYPE": "int"},
            ]
        elif "SHOW TABLES" in command:
            # probably, SnowflakeController.get_dimensions()
            result = [{"rows": 0, "bytes": 0}]
        else:
            result = []
        self_cursor._result = (x for x in result)  # this should be generator
        return self_cursor

    with patch.object(
        snowflake.connector.auth.Auth, "authenticate", new=_authenticate
    ), patch.object(
        snowflake.connector.cursor.SnowflakeCursor, "execute", new=_execute
    ), patch.object(
        snowflake.connector.network.SnowflakeRestful, "delete_session"
    ):
        yield execute_mock


def _snowflake_connect():
    return snowflake.connector.connect(
        user="test_user",
        password="test_password",
        account="test_account",
        warehouse="test_warehouse",
        database="test_database",
        region="test_region",
        role="test_role",
        schema="test_schema",
    )


def _run_simple_query_with_close_conn(mock_snowflake, log_tables, log_resource_usage):
    with snowflake_query_tracker(
        log_tables=log_tables,
        log_resource_usage=log_resource_usage,
        log_tables_with_preview=True,
        log_tables_with_schema=True,
    ) as st:
        query = "select * from " + TEST_TABLE_NAME
        with _snowflake_connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                cursor.fetchall()

            assert sorted(st.get_all_tables()) == [TEST_TABLE_NAME]

            session_id, query_id = st.get_last_session_with_query_id(many=False)
            session_id, query_ids = st.get_last_session_with_query_id(many=True)

            session_queries = st.get_all_session_queries().copy()
            # 1 session
            assert len(session_queries) == 1
            for st_session_id, st_query_ids in session_queries.items():
                assert len(st_query_ids) == 1

                assert (st_session_id, st_query_ids[0]) == (session_id, query_id)
                assert (st_session_id, st_query_ids) == (session_id, query_ids)

            # query
            assert len(mock_snowflake.mock_calls) == 1
            assert mock_snowflake.mock_calls[0] == call(query)

    if log_resource_usage:
        # should be cleaned
        assert len(st.get_all_session_queries()) == 0
    else:
        # on exit from context manager, st.get_all_sessions() shouldn't be affected
        #  - resources/tables queries shouldn't be tracked anyway
        assert len(st.get_all_session_queries()) == len(session_queries)
        assert all(
            [a == b for a, b in zip(st.get_all_session_queries(), session_queries)]
        )

    return session_id, query_ids


def _run_simple_query_no_close_conn(mock_snowflake, log_tables, log_resource_usage):
    with snowflake_query_tracker(
        log_tables=log_tables,
        log_resource_usage=log_resource_usage,
        log_tables_with_preview=True,
        log_tables_with_schema=True,
    ) as st:
        query = "select * from " + TEST_TABLE_NAME
        # with self._snowflake_connect() as conn:
        conn = _snowflake_connect()
        with conn.cursor() as cursor:
            cursor.execute(query)
            cursor.fetchall()
            # we want COMMIT here to have same behavior/amount of queries
            # with case when connection is auto-closed (with context manager)
            cursor.execute("COMMIT")

        assert sorted(st.get_all_tables()) == [TEST_TABLE_NAME]

        session_id, query_id = st.get_last_session_with_query_id(many=False)
        session_id, query_ids = st.get_last_session_with_query_id(many=True)

        session_queries = st.get_all_session_queries().copy()
        # 1 session
        assert len(session_queries) == 1
        for st_session_id, st_query_ids in session_queries.items():
            assert len(st_query_ids) == 1

            assert (st_session_id, st_query_ids[0]) == (session_id, query_id)
            assert (st_session_id, st_query_ids) == (session_id, query_ids)

        # query + COMMIT
        assert len(mock_snowflake.mock_calls) == 2
        assert mock_snowflake.mock_calls[0] == call(query)

    if log_resource_usage:
        # should be cleaned
        assert len(st.get_all_session_queries()) == 0
    else:
        # on exit from context manager, st.get_all_sessions() shouldn't be affected
        #  - resources/tables queries shouldn't be tracked anyway
        assert len(st.get_all_session_queries()) == len(session_queries)
        assert all(
            [a == b for a, b in zip(st.get_all_session_queries(), session_queries)]
        )

    return session_id, query_ids


QUERY_RUNNERS = [_run_simple_query_no_close_conn, _run_simple_query_with_close_conn]


@pytest.mark.usefixtures(set_airflow_context.__name__)
class TestSnowflakeQueryTracker:
    @pytest.mark.parametrize("run_query", QUERY_RUNNERS)
    def test_no_auto_log(self, mock_snowflake, run_query):
        run_query(mock_snowflake, log_tables=False, log_resource_usage=False)

        # query + COMMIT
        assert len(mock_snowflake.mock_calls) == 2

    @pytest.mark.parametrize("run_query", QUERY_RUNNERS)
    def test_just_resource(self, mock_snowflake, run_query):
        session_id, (query_id,) = run_query(
            mock_snowflake, log_tables=False, log_resource_usage=True
        )

        # 1 for actual query + COMMIT
        #  + 1 for resource usage
        assert len(mock_snowflake.mock_calls) == 3

        resource_query = mock_snowflake.mock_calls[-1][1][0]
        assert str(session_id) in resource_query
        assert str(query_id) in resource_query

    @pytest.mark.parametrize("run_query", QUERY_RUNNERS)
    def test_resource_and_table(self, mock_snowflake, run_query):
        session_id, (query_id,) = run_query(
            mock_snowflake, log_tables=True, log_resource_usage=True
        )

        # 1 for actual query + COMMIT
        #  + 1 for resource usage
        #  + 3 for tables
        assert len(mock_snowflake.mock_calls) == 6

        for pattern in (
            "information_schema.columns",
            "TRY_HEX_DECODE_STRING",
            "SHOW TABLES",
        ):
            assert any(
                [
                    pattern in mock_call[1][0] or TEST_TABLE_NAME in mock_call[1][0]
                    for mock_call in mock_snowflake.mock_calls
                ]
            ), mock_snowflake.mock_calls

        resource_query = mock_snowflake.mock_calls[2][1][0]
        assert str(session_id) in resource_query
        assert str(query_id) in resource_query

    @pytest.mark.parametrize("run_query", QUERY_RUNNERS)
    def test_explicit_tables(self, mock_snowflake, run_query):
        track_tables = ["tableX", "tableY"]
        run_query(mock_snowflake, log_tables=track_tables, log_resource_usage=False)

        # 1 for actual query + COMMIT
        #  + 3 for tables * 2 (tableX, tableY)
        assert len(mock_snowflake.mock_calls) == 8

        for pattern in (
            "information_schema.columns",
            "TRY_HEX_DECODE_STRING",
            "SHOW TABLES",
        ):
            for table_name in track_tables:
                assert any(
                    [
                        pattern in call[1][0] and table_name in call[1][0]
                        for call in mock_snowflake.mock_calls
                    ]
                ), mock_snowflake.mock_calls

    def test_not_tracked_queries(self, mock_snowflake):
        with snowflake_query_tracker() as st:
            with _snowflake_connect() as conn, conn.cursor() as cursor:
                for sql in [
                    "create table something ()",
                    "alter table alter column ()",
                    "alter session set ...",
                ]:
                    cursor.execute(sql)

            assert len(st.get_all_tables()) == 0
            assert len(st.get_all_session_queries()) == 0
            assert st.get_last_session_with_query_id(many=True) == (None, [])
            assert st.get_last_session_with_query_id(many=False) == (None, None)

            # 1 for automatic "alert session autocommit=false" + 3 queries above
            assert len(mock_snowflake.mock_calls) == 4

        # should stay the same - no extra queries
        assert len(mock_snowflake.mock_calls) == 4


def test_extract_tables():
    queries = (
        ("select * from table0", ["table0"]),
        ("update table1 set a=0", ["table1"]),
        ("delete from table2", ["table2"]),
        ("insert into table3", ["table3"]),
        (
            """sEleCT * fRom table1
            inner jOin schema1.table2 on a=b
            inner joIn (select * from (table3) where a=b) x on
            """,
            ["table1", "schema1.table2", "table3"],
        ),
        (
            """
            WITH A AS (
                select * from table3
            ),
            B as (
                select * from table4
            )
            select * from A
            left join B on A.a = B.b
            inner join (C) on A.c = C.c
            """,
            ["table3", "table4", "C"],
        ),
        (
            """select * from "SALES_DEMO"."PUBLIC"."SALES" limit 50;""",
            ['"SALES_DEMO"."PUBLIC"."SALES"'],
        ),
    )
    for query, expected_tables in queries:
        tables = try_extract_tables(query)
        assert sorted(tables) == sorted(expected_tables), query
