import pytest
import sqlparse

from dbnd._core.constants import DbndTargetOperationType
from dbnd_snowflake.POC.sql_extract import Column, SqlQueryExtractor


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
        SELECT *
          FROM table0
        """,
            {
                DbndTargetOperationType.read: {
                    "table0.*": [Column(table="table0", name="*", alias="table0.*")]
                }
            },
        ),
        (
            """
        SELECT *
          FROM schema0.table0
        """,
            {
                DbndTargetOperationType.read: {
                    "schema0.table0.*": [
                        Column(
                            table="schema0.table0", name="*", alias="schema0.table0.*"
                        )
                    ]
                }
            },
        ),
        (
            """
        SELECT *
          FROM schema0.table0 as alias
        """,
            {
                DbndTargetOperationType.read: {
                    "schema0.table0.*": [
                        Column(
                            table="schema0.table0", name="*", alias="schema0.table0.*"
                        )
                    ]
                }
            },
        ),
        (
            """
        SELECT *
          FROM    database0.schema0.table0
        """,
            {
                DbndTargetOperationType.read: {
                    "database0.schema0.table0.*": [
                        Column(
                            table="database0.schema0.table0",
                            name="*",
                            alias="database0.schema0.table0.*",
                        )
                    ]
                }
            },
        ),
        (
            """
        SELECT a.col_1, a.col_2, a.col_3
          FROM    database0.schema0.table0 a
        """,
            {
                DbndTargetOperationType.read: {
                    "col_1": [
                        Column(
                            table="database0.schema0.table0",
                            name="col_1",
                            alias="col_1",
                        )
                    ],
                    "col_2": [
                        Column(
                            table="database0.schema0.table0",
                            name="col_2",
                            alias="col_2",
                        )
                    ],
                    "col_3": [
                        Column(
                            table="database0.schema0.table0",
                            name="col_3",
                            alias="col_3",
                        )
                    ],
                }
            },
        ),
    ],
)
def test_extract_tables_operations_select(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


def parse_first_query(sqlquery):
    return sqlparse.parse(sqlquery)[0]


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
SELECT a.col0, b.col1, b.col2
  FROM table0 as a
  JOIN table1 as b
    ON t1.col0 = t2.col0
""",
            {
                DbndTargetOperationType.read: {
                    "col0": [Column(table="table0", name="col0", alias="col0")],
                    "col1": [Column(table="table1", name="col1", alias="col1")],
                    "col2": [Column(table="table1", name="col2", alias="col2")],
                }
            },
        ),
        (
            """
        SELECT table0.col0, table1.col1, table1.col2
          FROM table0
         INNER JOIN table1
            ON t1.col0 = t2.col0
        """,
            {
                DbndTargetOperationType.read: {
                    "col0": [Column(table="table0", name="col0", alias="col0")],
                    "col1": [Column(table="table1", name="col1", alias="col1")],
                    "col2": [Column(table="table1", name="col2", alias="col2")],
                }
            },
        ),
    ],
)
def test_extract_tables_operations_join(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.xfail()
@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
        SELECT a.col0, b.col1, b.col2, count(*), count(distinct a.col0) as x, sum(a.col2), sum(CASE WHEN a.col0 > 250 then 1 else 0 end) as d
          FROM table0 as a
          JOIN table1 as b
            ON t1.col0 = t2.col0
            group by a.col0, b.col1, b.col2
        """,
            {
                DbndTargetOperationType.read: {
                    "col0": [Column(table="table0", name="col0", alias="col0")],
                    "col1": [Column(table="table1", name="col1", alias="col1")],
                    "col2": [Column(table="table1", name="col2", alias="col2")],
                    "count(*)": [Column(table=None, name="count(*)", alias="count(*)")],
                    "x": [Column(table=None, name="x", alias="x")],
                    "sum(a.col2)": [
                        Column(table=None, name="sum(a.col2)", alias="sum(a.col2)")
                    ],
                    "d": [Column(table=None, name="d", alias="d")],
                }
            },
        ),
        (
            """
SELECT CASE WHEN a.col0 > 250 then 1 else 0 end as x
  FROM table0 as a
    group by a.col0, b.col1, b.col2
""",
            {
                DbndTargetOperationType.read: {
                    "x": [Column(table=None, name="x", alias="x")],
                }
            },
        ),
    ],
)
def test_extract_tables_operations_aggregate_functions(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


# todo fix
@pytest.mark.parametrize(
    "sqlquery,expected",
    [
        (
            """
            WITH cte0(col0, col1, col2) as (
            select v1.col0, v2.col1, v1.col2
            from schema0.table0 as v1
                     join schema1.table1 as v2
                          on v2.name = v1.name
            where v1.time > today
              and v2.time < today
            union
            select v4.col0, v4.col1, v4.col2
            from schema0.table5 as v4
                     join schema3.table4 as v5
                          on v4.id = v5.id
            ),
             cte2 as (
                 select *
                 from cte0
                          left outer join  schema1.table1 as v20
                                          on v20.id = cte0.col0
             )
            select *
            from cte2
            ;
            """,
            {
                DbndTargetOperationType.read: {
                    "col0": [
                        Column(table="schema0.table0", name="col0", alias="col0"),
                        Column(table="schema0.table5", name="col0", alias="col0"),
                    ],
                    "col1": [
                        Column(table="schema1.table1", name="col1", alias="col1"),
                        Column(table="schema0.table5", name="col1", alias="col1"),
                    ],
                    "col2": [
                        Column(table="schema0.table0", name="col2", alias="col2"),
                        Column(table="schema0.table5", name="col2", alias="col2"),
                    ],
                    "schema1.table1.*": [
                        Column(
                            table="schema1.table1", name="*", alias="schema1.table1.*"
                        )
                    ],
                }
            },
        ),
        (
            """
            select
                col1, col2, a
            from (
                select
                    col1, col2, count(col3) as a
                from
                    table1
                group by
                    col1, col2
                    )
            where col1 < 3
            """,
            {
                DbndTargetOperationType.read: {
                    "a": [Column(table="table1", name="count", alias="a")],
                    "col1": [Column(table="table1", name="col1", alias="col1")],
                    "col2": [Column(table="table1", name="col2", alias="col2")],
                }
            },
        ),
        (
            """
            select
                col1, col2, col3
            from (
                select
                    col1, col2, count(col3)
                from
                    table1
                group by
                    col1, col2
                    ) as something
            where col1 < 3
            """,
            {
                DbndTargetOperationType.read: {
                    "col1": [Column(table="table1", name="col1", alias="col1")],
                    "col2": [Column(table="table1", name="col2", alias="col2")],
                    "col3": [Column(table="something", name="col3", alias="col3")],
                }
            },
        ),
    ],
)
def test_extract_tables_operations_complex_select(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
            SELECT *
              INTO table0
              FROM table1
            """,
            {
                DbndTargetOperationType.write: {
                    "table0.*": [Column(table="table0", name="*", alias="table0.*")]
                },
                DbndTargetOperationType.read: {
                    "table1.*": [Column(table="table1", name="*", alias="table1.*")]
                },
            },
        ),
        (
            """
        INSERT INTO table0 (col0, col1, col2)
        VALUES (val0, val1, val2)
        """,
            {
                DbndTargetOperationType.write: {
                    "col0": [Column(table="table0", name="col0", alias="col0")],
                    "col1": [Column(table="table0", name="col1", alias="col1")],
                    "col2": [Column(table="table0", name="col2", alias="col2")],
                }
            },
        ),
        (
            """
        INSERT INTO table1 (x, y, z)
        SELECT col0, col1, col2
          FROM table0
        """,
            {
                DbndTargetOperationType.write: {
                    "x": [Column(table="table1", name="x", alias="x")],
                    "y": [Column(table="table1", name="y", alias="y")],
                    "z": [Column(table="table1", name="z", alias="z")],
                },
                DbndTargetOperationType.read: {
                    "col0": [Column(table="table0", name="col0", alias="col0")],
                    "col1": [Column(table="table0", name="col1", alias="col1")],
                    "col2": [Column(table="table0", name="col2", alias="col2")],
                },
            },
        ),
        (
            """
        INSERT INTO db.s.table1
        SELECT col0, col1, col2
          FROM db.s.table0
        """,
            {
                DbndTargetOperationType.write: {
                    "db.s.table1.*": [
                        Column(table="db.s.table1", name="*", alias="db.s.table1.*")
                    ]
                },
                DbndTargetOperationType.read: {
                    "col0": [Column(table="db.s.table0", name="col0", alias="col0")],
                    "col1": [Column(table="db.s.table0", name="col1", alias="col1")],
                    "col2": [Column(table="db.s.table0", name="col2", alias="col2")],
                },
            },
        ),
    ],
)
def test_extract_tables_operations_insert(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
UPDATE Customers
SET ContactName = 'Alfred Schmidt', City= 'Frankfurt'
WHERE CustomerID = 1;
        """,
            {
                DbndTargetOperationType.write: {
                    "ContactName": [
                        Column(
                            table="Customers", name="ContactName", alias="ContactName"
                        )
                    ],
                    "City": [Column(table="Customers", name="City", alias="City")],
                },
            },
        )
    ],
)
def test_extract_tables_operations_update(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
copy into JSON_TABLE from s3://bucket/file.json CREDENTIALS = (AWS_KEY_ID = '12345' AWS_SECRET_KEY = '12345')
        """,
            {
                DbndTargetOperationType.read: {
                    "s3://bucket/file.json.*": [
                        Column(
                            table="s3://bucket/file.json",
                            name="*",
                            alias="s3://bucket/file.json.*",
                            is_file=True,
                        )
                    ]
                },
                DbndTargetOperationType.write: {
                    "JSON_TABLE.*": [
                        Column(
                            table="JSON_TABLE",
                            name="*",
                            alias="JSON_TABLE.*",
                            is_file=False,
                        )
                    ]
                },
            },
        )
    ],
)
def test_copy_into_from_external_file(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
copy into JSON_TABLE ("Company Name", "Description") from s3://bucket/file.json CREDENTIALS = (AWS_KEY_ID = '12345' AWS_SECRET_KEY = '12345')
        """,
            {
                DbndTargetOperationType.read: {
                    "s3://bucket/file.json.*": [
                        Column(
                            table="s3://bucket/file.json",
                            name="*",
                            alias="s3://bucket/file.json.*",
                            is_file=True,
                        )
                    ]
                },
                DbndTargetOperationType.write: {
                    "Company Name": [
                        Column(
                            table="JSON_TABLE",
                            name="Company Name",
                            alias="Company Name",
                            is_file=False,
                        )
                    ],
                    "Description": [
                        Column(
                            table="JSON_TABLE",
                            name="Description",
                            alias="Description",
                            is_file=False,
                        )
                    ],
                },
            },
        )
    ],
)
def test_copy_into_with_columns_from_external_file(sqlquery, expected):
    assert (
        SqlQueryExtractor().extract_operations_schemas(parse_first_query(sqlquery))
        == expected
    )
