# © Copyright Databand.ai, an IBM Company 2022

import re

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import attr

from sqlparse.sql import (
    Comparison,
    Function,
    Identifier,
    IdentifierList,
    Parenthesis,
    Token,
    TokenList,
)
from sqlparse.tokens import Keyword, Name, Whitespace, Wildcard

from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.utils.sql_tracker_common.utils import ddict2dict, strip_quotes


@attr.s
class Column:
    dataset_name = attr.ib()
    name = attr.ib(default=None)
    alias = attr.ib(default=None)
    is_file = attr.ib(default=False)
    is_stage = attr.ib(default=False)

    @property
    def is_wildcard(self):
        return self.name == "*"


Columns = List[Column]
Schema = Dict[str, Columns]

OP_TYPE = DbndTargetOperationType
READ = DbndTargetOperationType.read
WRITE = DbndTargetOperationType.write
READ_OPERATIONS = {"FROM"}
CLOUD_SERVICE_URI_REGEX = re.compile(r"s3://\S+|azure://\S+|gcs://\S+")

STAGE_REGEX = re.compile(r"@\S+")


def calculate_file_path_default(file_path):
    return file_path


class SqlQueryExtractor:
    def __init__(self, calculate_file_path=calculate_file_path_default):
        self.calculate_file_path = calculate_file_path

    def extract_operations_schemas(self, statement: TokenList) -> Dict[OP_TYPE, Schema]:
        """
        This function go over the statement and extract the used tables names and the operation that it is been done
        with them. The result is a list of pairs where the left element is the table name and the right one is the
        operation.
        """
        # `sqlparse` implementation details:
        # Using the changing `idx` (the index of the current token) to keep moving in the token list (statement),
        # after finding a current token. First idx is -1 because this is the first position outside the token list.
        # Each call to `token_next` or `token_next_by` using the current idx to start searching from the current
        # position
        idx = -1

        # holds the columns we find in the scope and not yet assigned to any schema
        columns: Columns = []
        # holds any dynamic table schema that is created in the scope.
        # dynamic tables are created by CTE statements
        dynamic_tables: Dict[str, Schema] = {}

        # collect the operations and their schemas
        operations = defaultdict(lambda: defaultdict(list))
        is_copy_into_handled = False

        while True:
            # each iteration we pick up the next token we didn't handle yet
            idx, token = self._next_non_empty_token(idx, statement)
            if not token:
                # done iterating over the sql statement
                # converting the result to regular dict
                return ddict2dict(operations)

            token: Token
            if token.ttype in Keyword:
                operation_name = token.value.upper()
                idx_potential, next_token = self._next_non_empty_token(idx, statement)
                next_operation_name = next_token.value.upper() if next_token else None
                # copy and copy into statement are handled once per query
                if (
                    operation_name == "INTO"
                    or (operation_name == "COPY" and next_operation_name != "INTO")
                    and not is_copy_into_handled
                ):
                    extracted, idx = self.handle_into(
                        idx_potential, next_token, statement
                    )
                    operations[WRITE].update(extracted)
                    # There could be few `COPY` tokens, we're processing only the first one
                    is_copy_into_handled = True

                elif operation_name in READ_OPERATIONS:
                    extracted, idx, columns = self.handle_read_operation(
                        idx_potential, next_token, columns, dynamic_tables
                    )
                    # to support UNION we extend the array of the same column
                    for k, v in extracted.items():
                        operations[READ][k].extend(v)

                elif token.ttype is Keyword.CTE:
                    # after a CTE statement (WITH) we expect identifiers list with each has a sub-query inside parenthesis
                    idx, token = self._next_non_empty_token(idx, statement)

                    if isinstance(token, IdentifierList):
                        token_list = token.get_identifiers()
                    else:
                        token_list = [token]

                    for identifier in token_list:
                        _, cte_statement = identifier.token_next_by(i=Parenthesis)
                        cte_operations = self.extract_operations_schemas(cte_statement)
                        extracted = self.enrich_with_dynamic(
                            cte_operations[READ], dynamic_tables
                        )
                        dynamic_tables[identifier.get_name()] = extracted

    def handle_read_operation(
        self,
        idx: int,
        next_token: Token,
        columns: Columns,
        dynamic_tables: Dict[str, Schema],
    ) -> Tuple[Schema, int, Columns]:
        extracted = {}
        if isinstance(next_token, Parenthesis):
            return self.handle_nested_query(next_token)
        if next_token.ttype not in Keyword and isinstance(next_token, Identifier):
            # no subquery - just parse the source/dest name of the operator
            extracted, left_columns = self.generate_schema(columns, next_token)
            columns = left_columns
            extracted = self.enrich_with_dynamic(extracted, dynamic_tables)

        return extracted, idx, columns

    def handle_nested_query(self, nested_statement):
        columns = []
        is_select_query = False
        nested_idx = 0
        for token in nested_statement.tokens:
            # identify nested query is select
            if token.normalized == "SELECT":
                is_select_query = True
            # only select nested queries are supported
            elif is_select_query:
                # TODO: we might want to parse query columns for nested read operations
                # if isinstance(token, IdentifierList):
                # columns.append(Column(dataset_name='',name=token.normalized, alias=token.normalized))
                if token.normalized in READ_OPERATIONS:
                    next = self._next_non_empty_token(nested_idx, nested_statement)
                    if not next:
                        continue
                    extracted, nested_idx, columns = self.handle_read_operation(
                        nested_idx, next[1], columns, {}
                    )
            nested_idx += 1
        return extracted, nested_idx, columns

    def clean_query(self, query: str) -> str:
        """
        Cleans apostrophe and quote characters from cloud url or stage in query to parse it successfully by sqlparse
         :param str query: query statement
        """

        command_list = query.split(" ")
        clean_query_list = []
        for command in command_list:
            if command and (self.find_cloud_regex(command) or self.is_stage(command)):
                command = strip_quotes(command)
            clean_query_list.append(command)
        return " ".join(clean_query_list).strip()

    def is_stage(self, table_name: str) -> str:
        """
        Returns a pattern of file stage if exits
        :param str table_name: name of a table

        """
        if table_name:
            return STAGE_REGEX.findall(table_name.split(".")[-1])

    def generate_schema(
        self, columns: Columns, next_token: Identifier
    ) -> Tuple[Schema, Columns]:
        """
        generate schema object and columns for table, file or stage

        :param Columns columns: name of a table we want to track
        :param Identifier next_token: query identifier

        """
        cloud_uri_path = self.extract_cloud_uri(next_token)
        if cloud_uri_path:
            table_alias = self.calculate_file_path(cloud_uri_path)
            table_name = table_alias
            columns = [
                Column(
                    dataset_name=table_name, name="*", alias=table_alias, is_file=True
                )
            ]
        else:
            table_alias = self.get_identifier_name(next_token)
            table_name = self.get_full_name(next_token)
            if self.is_stage(table_name):
                columns = [
                    Column(
                        dataset_name=table_name,
                        name="*",
                        alias=table_alias,
                        is_stage=True,
                    )
                ]
        extracted, left_columns = self.extract_schema(table_name, table_alias, columns)
        return extracted, left_columns

    def handle_into(
        self, idx: int, next_token: Token, statement: TokenList
    ) -> Tuple[Schema, int]:
        if next_token.is_group:
            # if token is group, get inner function token if exist
            for token in next_token.tokens:
                if isinstance(token, Function):
                    next_token = token
        table_name = self.get_identifier_name(next_token)
        table_full_name = self.get_full_name(next_token)

        if isinstance(next_token, Function):
            extracted = self.extract_write_schema(
                next_token.get_parameters(), table_name, table_name
            )
        else:
            extracted = self.extract_write_schema(
                [Token(Wildcard, "*")], table_full_name, table_name
            )
        return extracted, idx

    def extract_write_schema(
        self, tokens: List[Token], table_name: str, table_alias: str
    ) -> Schema:
        cols = self._extract_columns(tokens)
        extracted, _ = self.extract_schema(table_name, table_alias, cols)
        return extracted

    def extract_schema(
        self, table_name: str, table_alias: str, columns: Columns
    ) -> Tuple[Schema, Columns]:
        """
        Collects a schema and a wildcard column for a table, file or stage
        :param str table_name: full path and name of a table , file or stage
        :param str table_alias: alias of table, file or stage
        :param Columns columns: array of columns for a table, file or stage (for file and stage would be wildcard)
        """
        collected_schema = defaultdict(list)
        left_columns = []

        col: Column
        for col in columns:
            if col.is_wildcard:
                ev_col = attr.evolve(
                    col, dataset_name=table_name, alias=f"{table_name}.*"
                )
                collected_schema[ev_col.alias].append(ev_col)
                left_columns.append(col)

            elif col.dataset_name and col.dataset_name != table_alias:
                left_columns.append(col)

            else:
                ev_col = attr.evolve(col, dataset_name=table_name)
                collected_schema[ev_col.alias].append(ev_col)

        return collected_schema, left_columns

    @staticmethod
    def _next_non_empty_token(
        idx: int, token_list: TokenList
    ) -> Tuple[Optional[int], Optional[Token]]:
        return token_list.token_next(idx, skip_ws=True, skip_cm=True)

    def enrich_with_dynamic(
        self, extracted: Schema, dynamic: Dict[str, Schema]
    ) -> Schema:
        if not dynamic:
            return extracted

        result = defaultdict(list)
        for name, cols in extracted.items():
            for col in cols:
                if col.dataset_name in dynamic:
                    dynamic_table_schema = dynamic[col.dataset_name]
                    if col.is_wildcard:
                        result.update(dynamic_table_schema)

                    elif col.name in dynamic_table_schema:
                        result[col.name].extend(dynamic_table_schema[col.name])

                    else:
                        result[col.name].append(col)
                else:
                    result[name].append(col)
        return result

    def _extract_columns(self, tokens: List[Token]) -> Columns:
        collected_columns = []
        for token in tokens:
            if token.ttype == Wildcard:
                col = Column(dataset_name=None, name="*", alias=None)

            elif isinstance(token, Identifier):
                name = token.get_real_name()
                alias = token.get_alias() or name

                first = token.token_first()
                ref = (
                    first.value
                    if len(token.tokens) > 1 and first.ttype == Name
                    else None
                )
                col = Column(dataset_name=ref, name=name, alias=alias)

            elif isinstance(token, Comparison):
                name = token.left.get_real_name()
                col = Column(dataset_name=None, name=name, alias=name)

            else:
                name = token.normalized or token.get_alias()
                col = Column(dataset_name=None, name=name, alias=name)

            collected_columns.append(col)
        return collected_columns

    @staticmethod
    def get_identifier_name(identifier):
        return identifier.get_alias() or identifier.value.split(".")[-1]

    def extract_cloud_uri(self, identifier: Identifier) -> str:
        """
        Search for cloud providers pattern in query identifier,
        if pattern exists returns the URI as string

        :param Identifier identifier: query identifier
        """
        if identifier.value.lower() in ["s3", "azure", "gcs"]:
            return self.find_cloud_regex(identifier.parent.value)

    def find_cloud_regex(self, identifier: str) -> str:
        cloud_uri = CLOUD_SERVICE_URI_REGEX.findall(identifier.lower())
        return cloud_uri[0] if cloud_uri else None

    @staticmethod
    def get_full_name(identifier):
        # "name as alias" or "name alias" or "complicated column expression alias"
        ws_index, ws = identifier.token_next_by(t=Whitespace)
        if len(identifier.tokens) > 2 and ws is not None:
            return "".join(token.value for token in identifier.tokens[:ws_index])

        return identifier.value
