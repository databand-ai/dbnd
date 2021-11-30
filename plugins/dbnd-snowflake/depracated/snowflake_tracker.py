import contextlib
import functools

from collections import OrderedDict
from typing import Callable, ContextManager, Dict, List, Optional, Set, Union

import attr

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from dbnd_snowflake import log_snowflake_resource_usage, log_snowflake_table
from dbnd_snowflake.extract_sql_query import TableTargetOperation, extract_from_sql
from dbnd_snowflake.sql_utils import is_sql_crud_command
from targets.connections import build_conn_path


@attr.s
class SessionData(object):
    query_ids = attr.ib(factory=list)

    def is_empty(self):
        # if we have tables but no query ids?
        # return not self.query_ids and not self.tables  ???
        return not self.query_ids

    def add_query(self, query_id):
        if query_id:
            self.query_ids.append(query_id)


@attr.s
class ConnectionData(object):
    schema = attr.ib()
    database = attr.ib()
    connection = attr.ib(default=None)  # type: SnowflakeConnection
    tables_ops = attr.ib(factory=set)  # type: Set[TableTargetOperation]
    session_data = attr.ib(factory=OrderedDict)  # type: Dict[int, SessionData]
    connection_number = attr.ib(default=0)  # type: int

    def get_session_data(self, session_id):
        # type: (int) -> SessionData
        # we need this before `if` to correctly track nested track() calls
        if session_id not in self.session_data:
            self.session_data[session_id] = SessionData()
        return self.session_data[session_id]

    def reset_session_queries(self):
        self.session_data = OrderedDict()

    def reset_tables(self):
        self.tables_ops = set()

    def get_all_session_queries(self):
        return OrderedDict(
            [
                (session_id, session_data.query_ids)
                for session_id, session_data in self.session_data.items()
            ]
        )

    def get_connection_path(self):
        return build_conn_path(
            conn_type="snowflake",
            hostname=self.connection.host,
            port=self.connection.port,
            path="/".join(filter(None, [self.database, self.schema])),
        )


# track connection either when specific connection is closed or when leaving the context:
# Example 1:
# with snowflake_query_tracker():
#   with new_snowflake_connection() as conn1:
#     ... use conn1 ...
#     <if connection is closing - will track here>
#   conn2 = another_snowflake_connection():
#   ... use conn2 ... (wasn't closed)
# <track all not closed connections>
# Example 2:
# with snowflake_conn() as conn3:
#    with snowflake_query_tracker():
#      ... use conn3 ...
#       <track connection usage>
class SnowflakeQueryTracker(object):
    def __init__(
        self,
        track_connection_handler: Callable[[ConnectionData], None] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        self._connection_data = OrderedDict()  # type: Dict[int, ConnectionData]
        self._suspended = False
        self._track_connection_handler = track_connection_handler

        self.database = database
        self.schema = schema

        self.last_session_id = None
        self.last_session_query_ids = []

        self.connection_counter = 0

    def __enter__(self):
        self.patch_snowflake()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            # track all connections data when leaving the tracker scope (example 2)
            for cd in self._connection_data.values():
                self.track_connection_data(cd)
        finally:
            self.unpatch_snowflake()

    @contextlib.contextmanager
    def track_execute(self, cursor, command, *args, **kwargs):
        # type: (SnowflakeCursor, str, ..., ...) -> None

        if self._suspended or not is_sql_crud_command(command):
            yield
            return

        connection_data = self._get_connection_data(cursor.connection)

        session_id = cursor.connection.session_id
        session_data = connection_data.get_session_data(session_id)

        conn_path = connection_data.get_connection_path()
        tables_ops = extract_from_sql(conn_path, command)
        connection_data.tables_ops.update(tables_ops)

        try:
            yield
        finally:
            query_id = cursor.sfqid
            if query_id:
                session_data.add_query(query_id)

                # this is needed only to support self.get_last_session_with_query_id()
                # for backward compatibility, remove some day (2020/11/26)
                if self.last_session_id != session_id:
                    self.last_session_id = session_id
                    self.last_session_query_ids = []
                self.last_session_query_ids.append(query_id)

    def _get_connection_data(self, connection):
        # type: (SnowflakeConnection) -> ConnectionData
        conn_id = id(connection)
        if conn_id not in self._connection_data:
            self._connection_data[conn_id] = ConnectionData(
                database=connection.database or self.database,
                schema=connection.schema or self.schema,
                connection=connection,
                connection_number=self.connection_counter,
            )
            self.connection_counter += 1
        return self._connection_data[conn_id]

    def get_all_tables(self):
        tables = {
            table_op.name
            for connection_data in self._connection_data.values()
            for table_op in connection_data.tables_ops
        }
        return tables

    def get_all_session_queries(self):
        session_queries = OrderedDict()
        for connection_data in self._connection_data.values():  # type: ConnectionData
            session_queries.update(connection_data.get_all_session_queries())
        return session_queries

    def patch_snowflake(self):
        self._patch_cursor_execute()
        self._patch_connection_close()

    def _patch_cursor_execute(self):
        if hasattr(SnowflakeCursor.execute, "__dbnd_patched__"):
            # already patched
            # warn here ?
            return

        original = SnowflakeCursor.execute

        @functools.wraps(original)
        def snowflake_cursor_execute(cursor_self, *args, **kwargs):
            with self.track_execute(cursor_self, *args, **kwargs):
                return original(cursor_self, *args, **kwargs)

        snowflake_cursor_execute.__dbnd_patched__ = original
        SnowflakeCursor.execute = snowflake_cursor_execute

    def _patch_connection_close(self):
        if hasattr(SnowflakeConnection.close, "__dbnd_patched__"):
            # already patched
            # warn here ?
            return

        original = SnowflakeConnection.close

        @functools.wraps(original)
        def snowflake_connection_close(connection_self, *args, **kwargs):
            # track connection before closing it (Example 1)
            self.track_connection_data(self._connection_data.get(id(connection_self)))
            return original(connection_self, *args, **kwargs)

        snowflake_connection_close.__dbnd_patched__ = original  # or "= self" ?
        SnowflakeConnection.close = snowflake_connection_close

    def unpatch_snowflake(self):
        if hasattr(SnowflakeCursor.execute, "__dbnd_patched__"):
            SnowflakeCursor.execute = SnowflakeCursor.execute.__dbnd_patched__

        if hasattr(SnowflakeConnection.close, "__dbnd_patched__"):
            SnowflakeConnection.close = SnowflakeConnection.close.__dbnd_patched__

    def get_last_session_with_query_id(self, many):
        if not self.last_session_query_ids:
            return None, ([] if many else None)

        return (
            self.last_session_id,
            (self.last_session_query_ids if many else self.last_session_query_ids[-1]),
        )

    @contextlib.contextmanager
    def suspend(self):
        self._suspended = True
        yield
        self._suspended = False

    def track_connection_data(self, conn_data: ConnectionData):
        if not self._track_connection_handler or not conn_data:
            return
        if conn_data.connection.is_closed():
            # already closed, cannot proceed (and probably already tracked)
            return

        with self.suspend():
            self._track_connection_handler(conn_data)


def log_all_snowflake_resource_usage(cd, reset=True, **kwargs):
    # type: (ConnectionData, bool, ...) -> Optional[Dict[int, List[str]]]
    # copy to prevent resource usage queries to appear in current history
    history = cd.get_all_session_queries()
    for session_id, query_ids in history.items():
        log_snowflake_resource_usage(
            database=cd.database,
            connection_string=cd.connection,
            session_id=session_id,
            query_ids=query_ids,
            key=f"snowflake_query.{cd.connection_number}",
            **kwargs,
        )
    if reset:
        cd.reset_session_queries()
    return history


def log_all_snowflake_tables(cd, tables, reset=True, **kwargs):
    # type: (ConnectionData, Union[bool, List[str]], bool, ...) -> Optional[List[str]]
    """

    :param cd: ConnectionData object
    :param tables: if True - will log automatically discovered tables; if list of strings - will log only mentioned tables
    :param with_preview:
    :param with_schema:
    :param raise_on_error:
    :param reset: should reset the ConnectionData.tables once done
    :return:
    """
    if tables is True:
        tables = [table_op.name for table_op in cd.tables_ops]
    else:
        # don't reset if explicit tables provided
        reset = False

    if tables:
        for table in tables:
            parts = table.split(".")
            table_name = parts.pop()
            schema = parts.pop() if parts else cd.schema
            database = parts.pop() if parts else cd.database

            log_snowflake_table(
                table_name=table_name,
                schema=schema,
                database=database,
                connection_string=cd.connection,
                **kwargs,
            )
    if reset:
        cd.reset_tables()
    return tables


# TODO: better way of passing log_tables_*/log_resource_* parameters
#  maybe by passing partial log_snowflake_resource_usage()/log_snowflake_table()
#  with preset configuration values, or using SnowflakeConfig/explicit configs
@contextlib.contextmanager
def snowflake_query_tracker(
    log_resource_usage: bool = True,
    log_tables: Union[bool, List[str]] = True,
    log_tables_with_preview: Optional[bool] = None,
    log_tables_with_schema: Optional[bool] = None,
    log_resource_history_window: float = 15,
    log_resource_query_history_result_limit: Optional[int] = None,
    log_resource_delay: int = 1,
    log_resource_retries: int = 3,
    log_resource_retry_pause: float = 0,
    raise_on_error: bool = False,
    database=None,
    schema=None,
) -> ContextManager[SnowflakeQueryTracker]:
    """

    :param log_resource_usage: bool whether to track resource usage
    :param log_tables: either explicit list of tables to log or True to log all automatically extracted tables from queries
    :param log_tables_with_preview: to log tables preview
    :param log_tables_with_schema: to log tables schemas
    :param log_resource_history_window: How deep to search into QUERY_HISTORY. Set in minutes
    :param log_resource_query_history_result_limit: Passed through directly to QUERY_HISTORY search function as `RESULT_LIMIT` param
    :param log_resource_delay: Initial delay before looking in QUERY_HISTORY.
        Metadata can appear there with some delay. Use this param for fine tuning
    :param log_resource_retries: How much times to search in QUERY_HISTORY.
        Each time search is widened by increasing `RESULT_LIMIT` param.
    :param log_resource_retry_pause: Set number of seconds to pause before next retry.
    :param raise_on_error: By default all exceptions are muted so your task success status
        is not affected by errors in tracking. Set to true to re-raise all exceptions.
    :param database: database to use for resource usage
    :param schema: schema to use
    """

    def _track_connection(connection_data):
        # type: (ConnectionData) -> None
        if log_resource_usage:
            log_all_snowflake_resource_usage(
                connection_data,
                history_window=log_resource_history_window,
                query_history_result_limit=log_resource_query_history_result_limit,
                delay=log_resource_delay,
                retries=log_resource_retries,
                retry_pause=log_resource_retry_pause,
                raise_on_error=raise_on_error,
                reset=True,
            )
        if log_tables:
            log_all_snowflake_tables(
                connection_data,
                tables=log_tables,
                with_preview=log_tables_with_preview,
                with_schema=log_tables_with_schema,
                raise_on_error=raise_on_error,
                reset=True,
            )

    with SnowflakeQueryTracker(
        track_connection_handler=_track_connection, database=database, schema=schema,
    ) as st:
        yield st
