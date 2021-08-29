# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Based on SnowflakeOperator
import logging

from typing import Optional

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.utils.decorators import apply_defaults

from dbnd_snowflake import log_snowflake_resource_usage, log_snowflake_table


logger = logging.getLogger(__name__)


class LogSnowflakeTableOperator(SnowflakeOperator):
    """
    Queries Snowflake database for a table preview and metadata such as data schema
    and shape and tracks it into DBND

    :param str table: name of a table we want to track
    :param str key: Optional key for dbnd metrics. Defaults to table name
    :param with_preview: Set `True` to track table preview (first 20 rows)
    :param with_schema: Set `True` to track table schema, i.e. columns names, types and table dimensions
    :param str snowflake_conn_id: reference to specific snowflake connection id
    :param str warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param str database: name of database (will overwrite database defined
        in connection)
    :param str schema: name of schema (will overwrite schema defined in
        connection)
    :param str role: name of role (will overwrite any role defined in
        connection's extra JSON)
    """

    template_fields = ("table", "key")
    template_ext = ()
    ui_color = "#2B7BB3"

    @apply_defaults
    def __init__(
        self,
        table,
        key=None,
        with_preview: Optional[bool] = None,
        with_schema: Optional[bool] = None,
        raise_on_error: bool = False,
        *args,
        **kwargs
    ):
        super(LogSnowflakeTableOperator, self).__init__(*args, sql=None, **kwargs)

        self.table = table
        self.key = key
        self.with_preview = with_preview
        self.with_schema = with_schema
        self.raise_on_error = raise_on_error

    def execute(self, context):
        hook = self.get_hook()
        return log_snowflake_table(
            table_name=self.table,
            connection_string=hook.get_conn(),
            database=hook.database,
            schema=hook.schema,
            key=self.key,
            with_preview=self.with_preview,
            with_schema=self.with_schema,
            raise_on_error=self.raise_on_error,
        )


def log_snowflake_operator(
    op: SnowflakeOperator,
    table: str,
    with_preview: Optional[bool] = None,
    with_schema: Optional[bool] = None,
    raise_on_error: bool = False,
    **kwargs
):
    return LogSnowflakeTableOperator(
        table=table,
        snowflake_conn_id=kwargs.pop("snowflake_conn_id", op.snowflake_conn_id),
        database=kwargs.pop("database", op.database),
        warehouse=kwargs.pop("warehouse", op.warehouse),
        role=kwargs.pop("role", op.role),
        schema=kwargs.pop("schema", op.schema),
        task_id=kwargs.pop("task_id", "log_table_{}_{}".format(op.task_id, table)),
        with_preview=with_preview,
        with_schema=with_schema,
        raise_on_error=raise_on_error,
        **kwargs
    )


class LogSnowflakeResourceOperator(SnowflakeOperator):
    """
    Queries Snowflake database for resources used to execute specific SQL query and tracks it into DBND

    :param str snowflake_conn_id: reference to specific snowflake connection id
    :param session_id: Optional id of a Snowflake session used to execute query.
    :param query_id: Snowflake's QUERY_ID of a query executed previously you want to find and log resources.
        Use `run_snowflake()` helper to execute query and get it's session_id & query_id
    :param key: Optional key for dbnd metrics. Defaults to table name
    :param history_window: How deep to search in QUERY_HISTORY for a query. Set in minutes.
    :param retries: How deep to search in QUERY_HISTORY for a query. Set in minutes.
    :param retry_pause: How deep to search in QUERY_HISTORY for a query. Set in minutes.
    :param str warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param str database: name of database (will overwrite database defined
        in connection)
    :param str schema: name of schema (will overwrite schema defined in
        connection)
    :param str role: name of role (will overwrite any role defined in
        connection's extra JSON)
    """

    template_fields = ("session_id", "query_id", "key")
    template_ext = ()
    ui_color = "#2B7BB3"

    @apply_defaults
    def __init__(
        self,
        query_id: str,
        session_id: Optional[str] = None,
        key: Optional[str] = "snowflake_query",
        history_window: float = 15,
        query_history_result_limit: int = 100,
        retries: int = 3,
        retry_pause: int = 15,
        raise_on_error: bool = False,
        *args,
        **kwargs
    ):
        super(LogSnowflakeResourceOperator, self).__init__(sql=None, *args, **kwargs)

        self.session_id = session_id
        self.query_id = query_id
        self.key = key
        self.history_window = history_window
        self.query_history_result_limit = query_history_result_limit
        self.retries = retries
        self.retry_pause = retry_pause
        self.raise_on_error = raise_on_error

    def execute(self, context):
        hook = self.get_hook()

        log_snowflake_resource_usage(
            database=hook.database,
            connection_string=hook.get_conn(),
            query_ids=[self.query_id],
            session_id=int(self.session_id) if self.session_id else None,
            key=self.key,
            history_window=self.history_window,
            query_history_result_limit=self.query_history_result_limit,
            retries=self.retries,
            retry_pause=self.retry_pause,
            raise_on_error=self.raise_on_error,
        )


def log_snowflake_resource_operator(op: SnowflakeOperator, **kwargs):
    return LogSnowflakeResourceOperator(
        snowflake_conn_id=kwargs.pop("snowflake_conn_id", op.snowflake_conn_id),
        database=kwargs.pop("database", op.database),
        warehouse=kwargs.pop("warehouse", op.warehouse),
        role=kwargs.pop("role", op.role),
        schema=kwargs.pop("schema", op.schema),
        task_id=kwargs.pop("task_id", "log_resources_{}".format(op.task_id)),
        **kwargs
    )
