from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union

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
from sqlparse.tokens import Keyword, Name, Token as TokenType, Whitespace, Wildcard

from dbnd._core.constants import DbndTargetOperationType
from dbnd_snowflake.POC.utils import ddict2dict


@attr.s
class Column:
    table = attr.ib()
    name = attr.ib(default=None)
    alias = attr.ib(default=None)

    @property
    def is_wildcard(self):
        return self.name == "*"


Columns = List[Column]
Schema = Dict[str, Columns]

OP_TYPE = DbndTargetOperationType
READ = DbndTargetOperationType.read
WRITE = DbndTargetOperationType.write
READ_OPERATIONS = {
    "FROM",
    "LEFT JOIN",
    "JOIN",
    "INNER JOIN",
    "LEFT OUTER JOIN",
    "RIGHT JOIN",
    "RIGHT OUTER",
    "RIGHT OUTER JOIN",
}


class SqlQueryExtractor:
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
        while True:
            # each iteration we pick up the next token we didn't handle yet
            idx, token = self._next_non_empty_token(idx, statement)
            if not token:
                # done iterating over the sql statement
                # converting the result to regular dict for convince
                return ddict2dict(operations)

            token: Token
            if token.ttype in Keyword:
                operation_name = token.value.upper()
                idx_potential, next_token = self._next_non_empty_token(idx, statement)

                if operation_name == "SELECT":
                    extracted_columns, idx = self.handle_select(
                        idx_potential, next_token
                    )
                    columns.extend(extracted_columns)

                elif operation_name == "INTO":
                    extracted, idx = self.handle_into(
                        idx_potential, next_token, statement
                    )
                    operations[WRITE].update(extracted)

                elif operation_name == "UPDATE":
                    extracted, idx = self.handle_update(
                        idx_potential, next_token, statement
                    )
                    operations[WRITE].update(extracted)

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
                    for identifier in token.get_identifiers():
                        _, cte_statement = identifier.token_next_by(i=Parenthesis)
                        cte_operations = self.extract_operations_schemas(cte_statement)
                        extracted = self.enrich_with_dynamic(
                            cte_operations[READ], dynamic_tables
                        )
                        dynamic_tables[identifier.get_name()] = extracted

    def handle_select(self, idx: int, next_token: Token) -> Tuple[Columns, int]:
        tokens: List[Token] = (
            next_token.get_identifiers()
            if isinstance(next_token, IdentifierList)
            else [next_token]
        )
        columns = self._extract_columns(tokens)
        return columns, idx

    def handle_read_operation(
        self,
        idx: int,
        next_token: Token,
        columns: Columns,
        dynamic_tables: Dict[str, Schema],
    ) -> Tuple[Schema, int, Columns]:
        # looking for a sub-query first - it can be the current token or the first son of this token
        extracted = {}
        if isinstance(next_token, Parenthesis):
            # sub query doesnt have an alias name:
            # `... from (<sub_query>) where ...`
            extracted = self.extract_from_subquery(next_token, "anon", columns)

        elif isinstance(next_token, TokenList) and isinstance(
            next_token.token_first(), Parenthesis
        ):
            # the sub query has an alias name:
            # `... from (<sub_query>) as <alias_name> where ...`
            extracted = self.extract_from_subquery(
                next_token.token_first(), next_token.get_alias(), columns
            )

        elif next_token.ttype not in Keyword:
            # no subquery - just parse the source/dest name of the operator
            table_alias = self.get_identifier_name(next_token)
            table_name = self.get_full_name(next_token)
            extracted, left_columns = self.extract_schema(
                table_name, table_alias, columns
            )
            columns = left_columns
            extracted = self.enrich_with_dynamic(extracted, dynamic_tables)

        return extracted, idx, columns

    def handle_into(
        self, idx: int, next_token: Token, statement: TokenList
    ) -> Tuple[Schema, int]:
        table_alias = self.get_identifier_name(next_token)
        table_name = self.get_full_name(next_token)
        if isinstance(next_token, Function):
            extracted = self.extract_write_schema(
                next_token.get_parameters(), table_alias, table_alias
            )
        else:
            extracted = self.extract_write_schema(
                [Token(Wildcard, "*")], table_name, table_alias
            )
        return extracted, idx

    def handle_update(
        self, idx: int, next_token: Token, statement: TokenList
    ) -> Tuple[Schema, int]:
        table_alias = next_token.get_name()
        table_name = next_token.normalized

        idx, token = self._next_non_empty_token(idx, statement)
        operation_name = token.value.upper()

        extracted = {}
        if operation_name == "SET":
            idx, token = self._next_non_empty_token(idx, statement)
            extracted = self.extract_write_schema(
                token.get_identifiers(), table_name, table_alias
            )

        return extracted, idx

    def extract_write_schema(
        self, tokens: List[Token], table_name: str, table_alias: str
    ) -> Schema:
        cols = self._extract_columns(tokens)
        extracted, _ = self.extract_schema(table_name, table_alias, cols)
        return extracted

    def extract_from_subquery(
        self, tok: TokenList, table_alias: str, columns: Columns
    ) -> Schema:
        dynamic = self.extract_operations_schemas(tok)
        extracted, left_columns = self.extract_schema(table_alias, table_alias, columns)
        extracted = self.enrich_with_dynamic(extracted, {table_alias: dynamic[READ]})
        return extracted

    def extract_schema(
        self, table_name: str, table_alias: str, columns: Columns
    ) -> Tuple[Schema, Columns]:
        collected_schema = defaultdict(list)
        left_columns = []

        col: Column
        for col in columns:
            if col.is_wildcard:
                ev_col = attr.evolve(col, table=table_name, alias=f"{table_name}.*")
                collected_schema[ev_col.alias].append(ev_col)
                left_columns.append(col)

            elif col.table and col.table != table_alias:
                left_columns.append(col)

            else:
                ev_col = attr.evolve(col, table=table_name)
                collected_schema[ev_col.alias].append(ev_col)

        return collected_schema, left_columns

    @staticmethod
    def _next_non_empty_token(
        idx: int, token_list: TokenList
    ) -> Tuple[Optional[int], Optional[Token]]:
        return token_list.token_next(idx, skip_ws=True, skip_cm=True)

    def enrich_with_dynamic(
        self, extracted: Schema, dynamic: Dict[str, Schema],
    ) -> Schema:
        if not dynamic:
            return extracted

        result = defaultdict(list)
        for name, cols in extracted.items():
            for col in cols:
                if col.table in dynamic:
                    dynamic_table_schema = dynamic[col.table]
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
                col = Column(table=None, name="*", alias=None)

            elif isinstance(token, Identifier):
                name = token.get_real_name()
                alias = token.get_alias() or name

                first = token.token_first()
                ref = (
                    first.value
                    if len(token.tokens) > 1 and first.ttype == Name
                    else None
                )
                col = Column(table=ref, name=name, alias=alias)

            elif isinstance(token, Comparison):
                name = token.left.get_real_name()
                col = Column(table=None, name=name, alias=name)

            else:
                name = token.get_alias() or token.normalized
                col = Column(table=None, name=name, alias=name)

            collected_columns.append(col)
        return collected_columns

    @staticmethod
    def get_identifier_name(identifier):
        return identifier.get_alias() or identifier.value.split(".")[-1]

    @staticmethod
    def get_full_name(identifier):
        # "name as alias" or "name alias" or "complicated column expression alias"
        ws_index, ws = identifier.token_next_by(t=Whitespace)
        if len(identifier.tokens) > 2 and ws is not None:
            return "".join(token.value for token in identifier.tokens[:ws_index])

        return identifier.value
