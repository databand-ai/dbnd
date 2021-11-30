from typing import List, Optional, Set, Tuple

import attr
import sqlparse

from sqlparse import tokens as T
from sqlparse.sql import Comparison, IdentifierList, Parenthesis, TokenList
from sqlparse.tokens import CTE, Keyword, Token

from dbnd._core.constants import DbndTargetOperationType


OPERATIONS = {
    # read
    "FROM": DbndTargetOperationType.read,
    "LEFT JOIN": DbndTargetOperationType.read,
    "JOIN": DbndTargetOperationType.read,
    "INNER JOIN": DbndTargetOperationType.read,
    "LEFT OUTER JOIN": DbndTargetOperationType.read,
    "RIGHT JOIN": DbndTargetOperationType.read,
    "RIGHT OUTER": DbndTargetOperationType.read,
    "RIGHT OUTER JOIN": DbndTargetOperationType.read,
    "COPY": DbndTargetOperationType.read,  # Support `COPY {source} TO {dest}`
    "USING": DbndTargetOperationType.read,  # Support `MERGE INTO {dest} USING {source}`
    # write
    "INTO": DbndTargetOperationType.write,  # Support `COPY INTO`, `SELECT INTO`, `MERGE INTO {dest} USING {source}`
    "INSERT INTO": DbndTargetOperationType.write,
    "UPDATE": DbndTargetOperationType.write,
    "TO": DbndTargetOperationType.write,  # Support `COPY {source} TO {dest}`
}
SEQUENCE_OPERATIONS = {
    ("DELETE", "FROM"): DbndTargetOperationType.delete,
    ("TRUNCATE", "TABLE"): DbndTargetOperationType.delete,
}

# tuple of: (table_name, target_operation)
TableOperation = Tuple[str, DbndTargetOperationType]


@attr.s(frozen=True)
class TableTargetOperation(object):
    path = attr.ib(default=None)  # type: str
    name = attr.ib(default=None)  # type: str
    operation = attr.ib(default=None)  # type:DbndTargetOperationType
    success = attr.ib(default=True)  # type: bool


def detect_cte_tables(statement):
    # type: (TokenList) -> Set[TableOperation]
    cte_names = set()

    idx, token = _next_non_empty_token(-1, statement)
    if not (token and token.ttype == CTE):
        return cte_names

    idx, token = _next_non_empty_token(idx, statement)
    if isinstance(token, IdentifierList):
        for identifier in token.get_identifiers():
            name = identifier.get_real_name()
            if name:
                cte_names.add(name)

    return cte_names


def extract_from_sql(path, sqlquery):
    # type: (str, str) -> Set[TableTargetOperation]
    """
    Find every tables in the sqlquery and build a ready to report target with - path, name and operation

    @param path: string representing the location of the database
    @param sqlquery: string of the sql-query
    @return: Set of targets operation to log with (full_path, name, target_operation)
    """
    # assuming there is only one statement in the sqlquery
    statement = sqlparse.parse(sqlquery)[0]

    table_operations = extract_tables_operations(statement)
    cte_tables = detect_cte_tables(statement)

    result = set()
    for name, op in table_operations:
        # don't return cte_tables
        if name not in cte_tables:
            parsed_path, name = build_target_path(path, name)
            operation = TableTargetOperation(path=parsed_path, name=name, operation=op)
            result.add(operation)

    return result


def extract_tables_operations(statement):
    # type: (TokenList) -> List[TableOperation]
    """
    This function go over the statement and extract the used tables names and the operation that it is been done with them.
    The result is a list of pairs where the left element is the table name and the right one is the operation.
    """
    # `sqlparse` implementation details:
    # using the changing `idx` (the index of the token) to keep moving in the token list after finding a current token
    # first idx is -1 because this is the first position outside the token list
    # each call to `token_next` or `token_next_by` using the current idx to start searching from the current position
    idx = -1
    tables_operators = []
    while True:
        # each iteration we pick up the next token we didn't handle yet
        idx, token = _next_non_empty_token(idx, statement)
        if not token:
            # done iterating over the sql statement
            return tables_operators

        if token.ttype in Keyword:
            operation_name = token.value.upper()
            idx_potential, next_token = _next_non_empty_token(idx, statement)

            # if the following token is another keyword its usually has different meaning than we expect
            if operation_name in OPERATIONS:
                # handle basic read/write operations

                # looking for a sub-query first - it can be the current token or the first son of this token
                if isinstance(next_token, Parenthesis):
                    # sub query doesnt have an alias name:
                    # `... from (<sub_query>) where ...`
                    extracted = extract_tables_operations(next_token)
                    tables_operators.extend(extracted)
                    idx = idx_potential

                elif isinstance(next_token, TokenList) and isinstance(
                    next_token.token_first(), Parenthesis
                ):
                    # the sub query has an alias name:
                    # `... from (<sub_query>) as <alias_name> where ...`
                    extracted = extract_tables_operations(next_token.token_first())
                    tables_operators.extend(extracted)
                    idx = idx_potential

                elif next_token.ttype not in Keyword:
                    # no subquery - just parse the source/dest name of the operator
                    op = OPERATIONS[operation_name]
                    name = _extract_token_name(next_token)
                    extracted = [(name, op)]

                    tables_operators.extend(extracted)
                    idx = idx_potential

            elif (
                next_token.ttype in Keyword
                and (operation_name, next_token.value.upper()) in SEQUENCE_OPERATIONS
            ):
                op = SEQUENCE_OPERATIONS[(operation_name, next_token.value.upper())]
                idx, next_token = _next_non_empty_token(idx_potential, statement)
                name = _extract_token_name(next_token)
                tables_operators.append((name, op))

            elif token.ttype is Keyword.CTE:
                idx, extracted = _handle_cte_statement(idx, statement)
                tables_operators.extend(extracted)

            elif operation_name.startswith("CREATE"):
                idx, extracted = _handle_create_statement(idx, statement)
                tables_operators.extend(extracted)


def _handle_create_statement(idx, statement):
    # type: (int, TokenList) -> (int, List[TableOperation])
    """
    Handle the tables extraction from a create statement that in index=`idx` in `statement`
    Supported:
        * CREATE [OR REPLACE] [TEMPORARY] STAGE <table> [URL=<source>]
        * CREATE [OR REPLACE] TABLE <table>
    """

    tables_operators = []
    idx_potential, next_token = _next_non_empty_token(idx, statement)

    if next_token.match(Keyword, "TEMPORARY"):
        idx_potential, next_token = _next_non_empty_token(idx_potential, statement)

    if next_token.value.upper().startswith("STAGE"):
        name = "@{}".format(_extract_token_name(next_token, pos=1))
        tables_operators.append((name, DbndTargetOperationType.write))
        idx = idx_potential

        idx_potential, t = statement.token_next_by(m=(None, "(?i)url", True), idx=idx)
        if t:
            tables_operators.append((t.right.value, DbndTargetOperationType.read))
            idx = idx_potential

    elif next_token.value.upper().startswith("TABLE"):
        idx, t = _next_non_empty_token(idx_potential, statement)
        name = _extract_token_name(t)
        tables_operators.append((name, DbndTargetOperationType.write))

    return idx, tables_operators


def _handle_cte_statement(idx, statement):
    # type: (int, TokenList) -> (int, List[TableOperation])
    """
    Handle the tables extraction from a CTE statement that in index=`idx` in `statement`
    Supported:
        * WITH cte_name_1 as (<query_1>), cte_name_2 as (<query_2>), ..., cte_name_'n as (<query_'n>)
    """

    # after a CTE statement (WITH) we expect identifiers list with each has a sub-query inside parenthesis
    tables_operators = []

    idx, token = _next_non_empty_token(idx, statement)
    for identifier in token.get_identifiers():
        _, p = identifier.token_next_by(i=Parenthesis)
        sub_query_operations = extract_tables_operations(p)
        tables_operators.extend(sub_query_operations)

    return idx, tables_operators


def _extract_token_name(token, pos=0):
    # type: (Token, int) -> str
    return token.value.split(" ")[pos]


def _next_non_empty_token(idx, token_list):
    # type: (int, TokenList) -> Optional[Token]
    return token_list.token_next(idx, skip_ws=True, skip_cm=True)


def build_target_path(base_path, name):
    # type: (str, str) -> (str, str)
    """
    Building a target path and a name for the extracted location from sql-query
    Supporting tables with schemas and database, different file systems and Snowflake staging

    @param base_path: string representing the location of the database
    @param name: the extracted name from the sql-query
    @return: The full path of the target and the name of the target
    """
    name = name.strip("\"'")

    if "://" in name:
        # S3://bucket/key | GCS://bucket/key ...
        path = name
        _, _, name = name.partition("://")
        return path, name

    if ":\\" in name:
        # D:\\folder\folder\file
        path = name
        _, _, name = name.partition(":\\")
        return path, name

    if name.startswith("@"):
        # Snowflake staging
        name = name.strip("@")
        name, destination = parse_parts(name)
        name = "@{}".format(name)
        path = "{}/staging/{}".format(base_path, destination)
        return path, name

    name, destination = parse_parts(name)

    return "{}/{}".format(base_path, destination), name


def parse_parts(name):
    parts = list(map(lambda s: s.strip("\"'"), name.split(".")))
    destination = "/".join(map(str.lower, parts))
    name = ".".join(parts)
    return name, destination
