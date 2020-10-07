import mock

from dbnd_postgres.postgres_contreoller import PostgresController
from dbnd_postgres.postgres_values import PostgresTable, PostgresTableValueType
from targets.value_meta import ValueMetaConf


def postgres_controller_mock():
    res = mock.MagicMock(PostgresController)
    res.get_column_types.return_value = {"name": "varchar"}
    res.get_histograms_and_stats.return_value = {}, {}
    res.to_preview.return_value = "test preview"
    res.return_value = res  # Mock constructor
    res.__enter__.return_value = res  # mock context manager
    return res


class TestPostgresTableValueType:
    def test_df_value_meta(self):
        # Arrange
        postgres_table = PostgresTable(
            table_name="test_table", connection_string="user@test.db"
        )
        meta_conf = ValueMetaConf.enabled()

        with mock.patch(
            "dbnd_postgres.postgres_values.PostgresController",
            new_callable=postgres_controller_mock,
        ) as postgres:
            # Act
            PostgresTableValueType().get_value_meta(postgres_table, meta_conf=meta_conf)

            # Assert
            assert postgres.get_column_types.called
            assert postgres.get_histograms_and_stats.called
            assert postgres.to_preview.called


class TestPostgresController:
    def test_get_histograms_and_stats(self):
        with mock.patch(
            "dbnd_postgres.postgres_values.PostgresController._query"
        ) as query_patch:
            # Arrange
            pg_stats_data = [
                {
                    "attname": "customer",
                    "null_frac": 0.5,
                    "n_distinct": 8,
                    "most_common_vals": "{customerA, customerB}",
                    "most_common_freqs": [0.2, 0.2],
                }
            ]
            pg_class_data = [{"reltuples": 10}]
            information_schema_columns_data = [
                {"column_name": "customer", "data_type": "varchar"}
            ]
            query_patch.side_effect = [
                pg_stats_data,
                pg_class_data,
                information_schema_columns_data,
            ]

            expected_stats = {
                "customer": {
                    "count": 10,
                    "distinct": 8,
                    "type": "varchar",
                    "null-count": 5,
                }
            }
            expected_histograms = {
                "customer": ([2, 2, 1], ["customerA", "customerB", "_others"])
            }

            # Act
            postgres = PostgresController("user@database", "data_table")
            meta_conf = ValueMetaConf.enabled()
            stats, histograms = postgres.get_histograms_and_stats(meta_conf)

            # Assert
            assert stats == expected_stats
            assert histograms == expected_histograms
