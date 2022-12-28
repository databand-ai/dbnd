# © Copyright Databand.ai, an IBM Company 2022
from targets.marshalling import register_marshaller
from targets.marshalling.marshaller_loader import dbnd_package_marshaller
from targets.target_config import FileFormat
from targets.values import ValueTypeLoader, register_value_type


def register_value_type_pandas():
    ### Pandas Data Frame

    value_type_pandas_df = register_value_type(
        ValueTypeLoader(
            "pandas.core.frame.DataFrame",
            "targets.providers.pandas.pandas_values.DataFrameValueType",
            "dbnd",
            type_str_extras=["pd.DataFrame", "DataFrame"],
        )
    )

    value_type_pandas_df.register_marshallers(
        {
            FileFormat.txt: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToCsv"
            ),
            FileFormat.csv: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToCsv"
            ),
            FileFormat.table: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToTable"
            ),
            FileFormat.parquet: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToParquet"
            ),
            FileFormat.feather: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToFeather"
            ),
            FileFormat.hdf5: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToHdf5"
            ),
            FileFormat.pickle: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToPickle"
            ),
            FileFormat.json: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToJson"
            ),
            FileFormat.tsv: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToTsv"
            ),
            FileFormat.excel: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToExcel"
            ),
        }
    )

    value_type_pandas_series = register_value_type(
        ValueTypeLoader(
            "pandas.core.series.Series",
            "targets.providers.pandas.pandas_values.PandasSeriesValueType",
            "dbnd",
            type_str_extras=["pd.Series"],
        )
    )
    value_type_pandas_series.register_marshallers(
        {
            FileFormat.csv: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameAsSeriesToCsv"
            ),
            FileFormat.table: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToTable"
            ),
            FileFormat.parquet: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToParquet"
            ),
            FileFormat.feather: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToFeather"
            ),
            FileFormat.hdf5: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToHdf5"
            ),
            FileFormat.pickle: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToPickle"
            ),
            FileFormat.json: dbnd_package_marshaller(
                "targets.providers.pandas.pandas_marshaller.DataFrameToJson"
            ),
        }
    )

    # orchestration only
    value_type_dict_df = register_value_type(
        ValueTypeLoader(
            "typing.Dict[str, pandas.core.frame.DataFrame]",
            "targets.providers.pandas.pandas_values.DataFramesDictValueType",
            "dbnd",
            type_str_extras=[
                "DataFrameDict",
                "Dict[str,pd.DataFrame]",
                "Dict[str,DataFrame]",
            ],
        )
    )
    value_type_dict_df.register_marshaller(
        FileFormat.hdf5,
        dbnd_package_marshaller(
            "targets.providers.pandas.pandas_marshaller.DataFrameDictToHdf5"
        ),
    )


def register_pd_to_hdf5_as_table_marshaler():
    register_marshaller(
        "pd.DataFrame",
        FileFormat.hdf5,
        "targets.providers.pandas.pandas_marshaller.DataFrameToHdf5Table",
    )
