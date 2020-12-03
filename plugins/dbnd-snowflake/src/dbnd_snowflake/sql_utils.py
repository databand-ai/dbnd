import re


def try_extract_tables(command):
    if not command:
        return

    # TODO: this is not the right way to do it! it's not good idea to
    #  parse SQL with regex! If this direction is good, it's better to
    #  use SQL parser library, for example https://github.com/andialbrecht/sqlparse

    # catch all usages:
    # select from X ..
    # .. join X ...
    # update X ...
    # insert into X ...
    # delete from X ...
    tables = re.findall(
        r"\b(?:FROM|JOIN|UPDATE|INSERT\s+(?:\bINTO\b)?|DELETE\s+(?:\bFROM\b)?)[\s(]+([\"a-zA-Z._\d]+)\b",
        command,
        re.IGNORECASE,
    )

    # if code includes "with" statement, we should remove those tmp "tables", like:
    # WITH A as (select * from ...), B as (select * from ...) select ...

    # 1. first remove all inner select clauses to be able to catch A and B:
    # from query above, will keep only WITH A as , B as select ...
    cmd = command
    for i in range(20):
        new_cmd = re.sub(r"\([^()]*\)", "", cmd)
        if len(new_cmd) == len(cmd):
            break
        cmd = new_cmd

    # doesn't support recursive with (with recursive a(n) as ())
    # 2. extract only with-related part (WITH A as , B as)
    with_stmt_match = re.search(
        r"\bWITH\s+(([a-zA-Z._\d]+)\s+as\s*,?\s*)+", cmd, re.IGNORECASE,
    )
    if with_stmt_match:
        # 3. extract all "tables" names (A , B)
        with_stmt = re.findall(
            r"\b([a-zA-Z._\d]+)\s+as\b", with_stmt_match[0], re.IGNORECASE
        )
    else:
        with_stmt = []
    tables = [
        x
        for x in (set(tables) - set(with_stmt))
        if x.upper() not in ("SELECT", "TABLE")
    ]
    return tables


def is_sql_crud_command(command):
    # if all commands are ALTER (like ALTER SESSION or ALTER TABLE) or CREATE
    # (like CREATE TABLE) - don't track. Basically we'd want to track only queries
    # which retrieve or modify data
    # warning: ";" could be part of the some internal "where a like '%;%'"
    # but this shouldn't affect the correctness of the function
    for cmd in command.split(";"):
        if re.search(
            r"^\s*\b(SELECT|INSERT|DELETE|UPDATE|MERGE|WITH)\b", cmd, re.IGNORECASE
        ):
            return True
    return False
