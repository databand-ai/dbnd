import pytest
import sqlparse

from dbnd._core.constants import DbndTargetOperationType
from dbnd_snowflake.extract_sql_query import (
    TableTargetOperation,
    build_target_path,
    detect_cte_tables,
    extract_from_sql,
    extract_tables_operations,
)


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
        SELECT *
          FROM table0
        """,
            [("table0", DbndTargetOperationType.read)],
        ),
        (
            """
        SELECT *
          FROM schema0.table0
        """,
            [("schema0.table0", DbndTargetOperationType.read)],
        ),
        (
            """
        SELECT *
          FROM schema0.table0 as alias
        """,
            [("schema0.table0", DbndTargetOperationType.read)],
        ),
        (
            """SELECT *
          FROM    schema0.table0
        """,
            [("schema0.table0", DbndTargetOperationType.read)],
        ),
        (
            """
        SELECT *
          FROM    database0.schema0.table0
        """,
            [("database0.schema0.table0", DbndTargetOperationType.read)],
        ),
        (
            """
        SELECT col0, col1, col2
          FROM table0
          JOIN table1
            ON t1.col0 = t2.col0
        """,
            [
                ("table0", DbndTargetOperationType.read),
                ("table1", DbndTargetOperationType.read),
            ],
        ),
        (
            """
        SELECT col0, col1, col2
          FROM table0
         INNER JOIN table1
            ON t1.col0 = t2.col0
        """,
            [
                ("table0", DbndTargetOperationType.read),
                ("table1", DbndTargetOperationType.read),
            ],
        ),
        (
            """
        SELECT col0, col1, col2
          FROM table0
          LEFT JOIN schema3.table1
            ON t1.col0 = t2.col0
        """,
            [
                ("table0", DbndTargetOperationType.read),
                ("schema3.table1", DbndTargetOperationType.read),
            ],
        ),
        (
            """
        SELECT col0, col1, col2
          FROM table0
          LEFT OUTER JOIN table1
            ON t1.col0 = t2.col0
        """,
            [
                ("table0", DbndTargetOperationType.read),
                ("table1", DbndTargetOperationType.read),
            ],
        ),
        (
            """
        SELECT col0, col1, col2
          FROM table0
          RIGHT JOIN table1 as alias
            ON t1.col0 = t2.col0
        """,
            [
                ("table0", DbndTargetOperationType.read),
                ("table1", DbndTargetOperationType.read),
            ],
        ),
        (
            """
        SELECT col0, col1, col2
          FROM table0
          RIGHT OUTER JOIN table1
            ON t1.col0 = t2.col0
        """,
            [
                ("table0", DbndTargetOperationType.read),
                ("table1", DbndTargetOperationType.read),
            ],
        ),
    ],
)
def test_extract_tables_operations_select(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
            WITH cte0(col0, col1, col2) as (
            select v1.col0, v2.col1, v1.col2
            from schema0.table0 as v1
                     join schema1.table1 as v2
                          on v2.name = v1.name
                     join schema1.table2 as v3
                          on v2.id = v3.id
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
            [
                ("schema0.table0", DbndTargetOperationType.read),
                ("schema1.table1", DbndTargetOperationType.read),
                ("schema1.table2", DbndTargetOperationType.read),
                ("schema0.table5", DbndTargetOperationType.read),
                ("schema3.table4", DbndTargetOperationType.read),
                ("cte0", DbndTargetOperationType.read),
                ("schema1.table1", DbndTargetOperationType.read),
                ("cte2", DbndTargetOperationType.read),
            ],
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
                    )
            where col1 < 3
            """,
            [("table1", DbndTargetOperationType.read)],
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
            [("table1", DbndTargetOperationType.read)],
        ),
    ],
)
def test_extract_tables_operations_complex_select(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
            SELECT *
              INTO table0
              FROM table1
            """,
            [
                ("table0", DbndTargetOperationType.write),
                ("table1", DbndTargetOperationType.read),
            ],
        ),
        (
            """
        INSERT INTO table0 (col0, col1, col2)
        VALUES (val0, val1, val2)
        """,
            [("table0", DbndTargetOperationType.write)],
        ),
        (
            """
        INSERT INTO table1 (col0, col1, col2)
        SELECT col0, col1, col2
          FROM table0
        """,
            [
                ("table1", DbndTargetOperationType.write),
                ("table0", DbndTargetOperationType.read),
            ],
        ),
    ],
)
def test_extract_tables_operations_insert(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
    MERGE into "database"."public"."table" final_table using "database"."private"."staging_table" staging_table
        on final_table."id" = staging_table."id"
        WHEN matched
            THEN UPDATE SET
                final_table."col1" = staging_table."col1",
                final_table."col2" = staging_table."col2",
                final_table."col3" = staging_table."col3",
                final_table."col4" = staging_table."col4",

        WHEN not matched
            THEN insert (
              "col1", "col2", "col3", "col4")
                values (staging_table."col1",
                        staging_table."col2",
                        staging_table."col3",
                        staging_table."col4");
    """,
            [
                ('"database"."public"."table"', DbndTargetOperationType.write),
                ('"database"."private"."staging_table"', DbndTargetOperationType.read),
            ],
        )
    ],
)
def test_extract_tables_operations_merge(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
        UPDATE some_table
        SET some_column = some_value
        WHERE some_column = some_value
        """,
            [("some_table", DbndTargetOperationType.write)],
        )
    ],
)
def test_extract_tables_operations_update(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """COPY country TO '/usr1/proj/bray/sql/list_countries.copy' (DELIMITER '|')""",
            [
                ("country", DbndTargetOperationType.read),
                (
                    "'/usr1/proj/bray/sql/list_countries.copy'",
                    DbndTargetOperationType.write,
                ),
            ],
        ),
        (
            """COPY (SELECT * FROM country WHERE country_name LIKE 'A%') TO '/usr1/proj/bray/sql/a_list_countries.copy'""",
            [
                ("country", DbndTargetOperationType.read),
                (
                    "'/usr1/proj/bray/sql/a_list_countries.copy'",
                    DbndTargetOperationType.write,
                ),
            ],
        ),
        (
            """
            copy into mytable
            from 's3://mybucket/data/files'
            storage_integration = myint
            encryption=(master_key = 'eSxX0jzYfIamtnBKOEOwq80Au6NbSgPH5r4BDDwOaO8=')
            file_format = (format_name = my_csv_format)
            """,
            [
                ("mytable", DbndTargetOperationType.write),
                ("'s3://mybucket/data/files'", DbndTargetOperationType.read),
            ],
        ),
        (
            """
            copy into 'gcs://mybucket/unload/'
            from mytable
            storage_integration = myint
            file_format = (format_name = my_csv_format);
            """,
            [
                ("'gcs://mybucket/unload/'", DbndTargetOperationType.write),
                ("mytable", DbndTargetOperationType.read),
            ],
        ),
    ],
)
def test_extract_tables_operations_copy(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """delete from "SALES_DATA"."PUBLIC"."STAGING_TABLE";""",
            [('"SALES_DATA"."PUBLIC"."STAGING_TABLE"', DbndTargetOperationType.delete)],
        ),
        (
            """TRUNCATE TABLE "SALES_DATA"."PUBLIC"."STAGING_TABLE";""",
            [('"SALES_DATA"."PUBLIC"."STAGING_TABLE"', DbndTargetOperationType.delete)],
        ),
    ],
)
def test_extract_tables_operations_delete(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
            create table mytable_copy (b) as select * from mytable;
            """,
            [
                ("mytable_copy", DbndTargetOperationType.write),
                ("mytable", DbndTargetOperationType.read),
            ],
        )
    ],
)
def test_extract_tables_operations_create_table(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """
            create or replace stage sales_data.public.sales_data_stage
            URL = 's3://bucket/data/data_lineage/processed_data/2021-03-31_sales.csv'
            credentials = (
                AWS_KEY_ID = 'id'
                AWS_SECRET_KEY = 'secrete')
            file_format = "SALES_DATA"."PUBLIC"."SALES_CSV"
            """,
            [
                ("@sales_data.public.sales_data_stage", DbndTargetOperationType.write),
                (
                    "'s3://bucket/data/data_lineage/processed_data/2021-03-31_sales.csv'",
                    DbndTargetOperationType.read,
                ),
            ],
        ),
        (
            """
            create or replace temporary stage sales_data.public.sales_data_stage
            URL = 's3://bucket/data/data_lineage/processed_data/2021-03-31_sales.csv'
            credentials = (
                AWS_KEY_ID = 'id'
                AWS_SECRET_KEY = 'secrete')
            file_format = "SALES_DATA"."PUBLIC"."SALES_CSV"
            """,
            [
                ("@sales_data.public.sales_data_stage", DbndTargetOperationType.write),
                (
                    "'s3://bucket/data/data_lineage/processed_data/2021-03-31_sales.csv'",
                    DbndTargetOperationType.read,
                ),
            ],
        ),
    ],
)
def test_extract_tables_operations_create_staging(sqlquery, expected):
    assert extract_tables_operations(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """WITH cte0(col0, col1, col2) as (
                select v1.col0, v2.col1, v1.col2
                from schema0.table0 as v1
                         join schema1.table1 as v2
                              on v2.name = v1.name
                         join schema1.table2 as v3
                              on v2.id = v3.id
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
                              left outer join database1.schema2.table2
                                              on database1.schema2.table2.id = cte0.col0
                 )
            select *
            from cte2
        """,
            {"cte0", "cte2"},
        )
    ],
)
def test_detect_cte_tables(sqlquery, expected):
    assert detect_cte_tables(sqlparse.parse(sqlquery)[0]) == expected


@pytest.mark.parametrize(
    "sqlquery, expected",
    [
        (
            """WITH cte0(col0, col1, col2) as (
                select v1.col0, v2.col1, v1.col2
                from schema0.table0 as v1
                         join schema1.table1 as v2
                              on v2.name = v1.name
                         join schema1.table2 as v3
                              on v2.id = v3.id
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
        """,
            {
                TableTargetOperation(
                    path="snowflake://account.some.service/schema0/table0",
                    name="schema0.table0",
                    operation=DbndTargetOperationType.read,
                ),
                TableTargetOperation(
                    path="snowflake://account.some.service/schema1/table1",
                    name="schema1.table1",
                    operation=DbndTargetOperationType.read,
                ),
                TableTargetOperation(
                    path="snowflake://account.some.service/schema1/table2",
                    name="schema1.table2",
                    operation=DbndTargetOperationType.read,
                ),
                TableTargetOperation(
                    path="snowflake://account.some.service/schema0/table5",
                    name="schema0.table5",
                    operation=DbndTargetOperationType.read,
                ),
                TableTargetOperation(
                    path="snowflake://account.some.service/schema3/table4",
                    name="schema3.table4",
                    operation=DbndTargetOperationType.read,
                ),
            },
        ),
        (
            """copy into 'gcs://mybucket/unload'
  from mytable
  storage_integration = myint
  file_format = (format_name = my_csv_format);
""",
            {
                TableTargetOperation(
                    path="gcs://mybucket/unload",
                    name="mybucket/unload",
                    operation=DbndTargetOperationType.write,
                ),
                TableTargetOperation(
                    path="snowflake://account.some.service/mytable",
                    name="mytable",
                    operation=DbndTargetOperationType.read,
                ),
            },
        ),
    ],
)
def test_extract_from_sql(sqlquery, expected):
    assert extract_from_sql("snowflake://account.some.service", sqlquery) == expected


@pytest.mark.parametrize(
    "path, name, expected",
    [
        (
            "snowflake://account.some.service",
            '"SALES_DATA"."PUBLIC"."SALES_DATA"',
            (
                "snowflake://account.some.service/sales_data/public/sales_data",
                "SALES_DATA.PUBLIC.SALES_DATA",
            ),
        ),
        (
            "snowflake://account.some.service",
            "@sales_data.public.sales_data_stage",
            (
                "snowflake://account.some.service/staging/sales_data/public/sales_data_stage",
                "@sales_data.public.sales_data_stage",
            ),
        ),
        (
            "snowflake://account.some.service",
            "'s3://bucket/data/data_lineage/processed_data/2021-03-31_sales.csv'",
            (
                "s3://bucket/data/data_lineage/processed_data/2021-03-31_sales.csv",
                "bucket/data/data_lineage/processed_data/2021-03-31_sales.csv",
            ),
        ),
        (
            "mssql://scott/ms_2008",
            '"D:\directory\YourFileName.csv"',
            ("D:\\directory\\YourFileName.csv", "directory\\YourFileName.csv"),
        ),
    ],
)
def test_build_target_path(path, name, expected):
    assert build_target_path(path, name) == expected
