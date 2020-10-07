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

# Based on SnoflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.utils.decorators import apply_defaults

from dbnd._core.commands.metrics import log_snowflake_table
from dbnd_snowflake import log_snowflake_resource_usage


class LogSnowflakeTableOperator(SnowflakeOperator):
    """
    Queries Snowflake database for a table preview and metadata such as data schema
    and shape and tracks it into DBND

    :param str table: name of a table we want to track
    :param str key: Optional key for dbnd metrics. Defaults to table name
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

    @apply_defaults
    def __init__(self, table, key=None, account=None, *args, **kwargs):
        super(LogSnowflakeTableOperator, self).__init__(*args, sql=None, **kwargs)

        self.table = table
        self.key = key

        # TODO: deprecate
        self.account = account

    def get_hook(self):
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
        )

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = self.get_hook()

        connection_string = hook.get_uri()
        return log_snowflake_table(
            self.table, connection_string, hook.database, hook.schema, key=self.key
        )


def log_snowflake_operator(op: SnowflakeOperator, table, **kwargs):
    task_id = kwargs.pop("task_id", "log_table_%s_%s" % (op.task_id, table))
    return LogSnowflakeTableOperator(
        table=table,
        snowflake_conn_id=kwargs.pop("snowflake_conn_id", op.snowflake_conn_id),
        database=kwargs.pop("database", op.database),
        warehouse=kwargs.pop("warehouse", op.warehouse),
        role=kwargs.pop("role", op.role),
        schema=kwargs.pop("schema", op.schema),
        task_id=task_id,
        **kwargs
    )


class LogSnowflakeResourceOperator(SnowflakeOperator):
    """
    Queries Snowflake database for resources used to execute specific SQL query and tracks it into DBND

    :param str snowflake_conn_id: reference to specific snowflake connection id
    :param str session_id: Optional id of a Snowflake session used to execute query.
        Can be used if the same query is executed
    :param str key: Optional key for dbnd metrics. Defaults to table name
    :param sql: Exact sql query executed previously.
    :param str warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param str database: name of database (will overwrite database defined
        in connection)
    :param str schema: name of schema (will overwrite schema defined in
        connection)
    :param str role: name of role (will overwrite any role defined in
        connection's extra JSON)
    """

    @apply_defaults
    def __init__(self, session_id=None, key=None, account=None, *args, **kwargs):
        super(LogSnowflakeResourceOperator, self).__init__(*args, **kwargs)

        self.session_id = session_id
        self.key = key
        # TODO: deprecate
        self.account = account

    def get_hook(self):
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
        )

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = self.get_hook()

        conn_params = hook._get_conn_params()
        user = conn_params["user"]

        log_snowflake_resource_usage(
            self.sql,
            database=hook.database,
            user=user,
            connection_string=hook.get_uri(),
            session_id=self.session_id,
            key=self.key,
        )


def log_snowflake_resource_operator(op: SnowflakeOperator, **kwargs):
    task_id = kwargs.pop("task_id", "log_resources_%s" % (op.task_id))
    return LogSnowflakeResourceOperator(
        sql=op.sql,
        snowflake_conn_id=kwargs.pop("snowflake_conn_id", op.snowflake_conn_id),
        database=kwargs.pop("database", op.database),
        warehouse=kwargs.pop("warehouse", op.warehouse),
        role=kwargs.pop("role", op.role),
        schema=kwargs.pop("schema", op.schema),
        task_id=task_id,
        **kwargs
    )
