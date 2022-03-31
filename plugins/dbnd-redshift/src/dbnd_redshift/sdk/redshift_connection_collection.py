from collections import defaultdict
from typing import List, Union

import attr

from psycopg2.extensions import connection

from dbnd_redshift.sdk.redshift_values import RedshiftOperation
from dbnd_redshift.sdk.wrappers import (
    DbndConnectionWrapper,
    DbndCursorWrapper,
    PostgresConnectionWrapper,
)


def get_object_hash(obj):
    connection_hash = None
    if (
        isinstance(obj, DbndCursorWrapper)
        or isinstance(obj, DbndConnectionWrapper)
        or isinstance(obj, PostgresConnectionWrapper)
    ):
        connection_hash = hash(obj.connection)
    elif isinstance(obj, connection):
        connection_hash = hash(connection)

    return connection_hash


@attr.s(auto_attribs=True)
class PostgresConnectionRuntime:
    connection: PostgresConnectionWrapper
    operations: List[RedshiftOperation]


KeyTypeUnion = Union[
    DbndCursorWrapper, DbndConnectionWrapper, PostgresConnectionWrapper, connection
]


class RedshiftConnectionCollection(defaultdict):
    def __init__(self, default_factory=None, **kwargs):
        super().__init__(default_factory, **kwargs)

    def __getitem__(self, item):
        return super().__getitem__(get_object_hash(item))

    def __contains__(self, item):
        return super().__contains__(get_object_hash(item))

    def get(self, key):
        return super().get(get_object_hash(key))

    def new_connection(self, key: KeyTypeUnion, value: PostgresConnectionRuntime):
        """
        Add new connection to map
        :param key: one of KeyTypeUnion union types
        :param value: value for connection, PostgresConnectionRuntime
        :return: None
        """
        hashed_key = get_object_hash(key)
        if hashed_key:
            super().__setitem__(hashed_key, value)

    def get_connection(
        self, key: KeyTypeUnion, default: PostgresConnectionWrapper = None
    ) -> PostgresConnectionWrapper:
        """
        Get psycopg2 connection from selected connection runtime object
        :param key: one of KeyTypeUnion union types
        :param default: default value if doesn't exist in map
        :return: connection object (PostgresConnectionWrapper)
        """
        obj = self.get(key)
        if hasattr(obj, "connection"):
            return obj.connection
        else:
            return default

    def get_operations(self, key: KeyTypeUnion) -> List[RedshiftOperation]:
        """
        Gets operations for given key in map
        :param key: one of KeyTypeUnion union types
        :return: list of sql operations for selected connection
        """
        obj = self.get(key)
        if hasattr(obj, "operations"):
            return obj.operations

    def add_operations(self, key: KeyTypeUnion, operations: List[RedshiftOperation]):
        """
        Given a key and operations adds the operations to the PostgresConnectionRuntime object in the given key
        :param key: one of KeyTypeUnion union types
        :param operations: list of RedshiftOperation to add
        :return: None
        """
        obj = self.get(key)
        if hasattr(obj, "operations"):
            obj.operations.extend(operations)

    def clear_operations(self, key: KeyTypeUnion):
        """
        Given a key clears operations from the PostgresConnectionRuntime in the dict
        :param key: one of KeyTypeUnion union types
        :return: None
        """
        obj = self.get(key)
        if hasattr(obj, "operations"):
            obj.operations.clear()
