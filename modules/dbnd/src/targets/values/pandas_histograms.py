import json
import logging
import typing

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from pandas.core.dtypes.common import is_bool_dtype, is_numeric_dtype, is_string_dtype

from dbnd._core.tracking.schemas.column_stats import (
    ColumnStatsArgs,
    get_column_stats_by_col_name,
)


if typing.TYPE_CHECKING:
    from dbnd._core.tracking.log_data_request import LogDataRequest
    from targets.value_meta import ValueMetaConf

logger = logging.getLogger(__name__)


class PandasHistograms(object):
    """
    calculates histograms and stats on pandas dataframe.
    """

    def __init__(self, df, meta_conf):
        # type: (pd.DataFrame, ValueMetaConf) -> None
        self.df = df
        self.meta_conf = meta_conf

    def get_histograms_and_stats(
        self,
    ) -> Tuple[List[ColumnStatsArgs], Dict[str, List[List]]]:
        columns_stats, histograms = [], {}

        if self.meta_conf.log_stats:
            columns_stats = self._calculate_stats(self.df)

        if self.meta_conf.log_histograms:
            hist_column_names = self._get_column_names_from_request(
                self.df, self.meta_conf.log_histograms
            )
            df_histograms = self.df.filter(hist_column_names)
            if pd.__version__ > "1":
                # return_type="expand" and orient="list" are used to stabilize produced
                # histograms data structure across pandas v0 & v1
                histograms = df_histograms.apply(
                    self._calculate_histograms,
                    result_type="expand",
                    args=(columns_stats,),
                ).to_dict(orient="list")
            else:
                histograms = df_histograms.apply(
                    self._calculate_histograms, args=(columns_stats,)
                ).to_dict()
                histograms = {k: list(v) for k, v in histograms.items()}

        return columns_stats, histograms

    def _get_column_names_from_request(self, df, data_request):
        # type: (pd.DataFrame, LogDataRequest) -> List[str]
        column_names = list(data_request.include_columns)
        for column_name, column_type in df.dtypes.items():
            if data_request.include_all_string and is_string_dtype(column_type):
                column_names.append(column_name)
            elif data_request.include_all_boolean and is_bool_dtype(column_type):
                column_names.append(column_name)
            elif data_request.include_all_numeric and is_numeric_dtype(column_type):
                column_names.append(column_name)

        column_names = [
            column
            for column in column_names
            if column not in data_request.exclude_columns
        ]
        return column_names

    def _calculate_stats(self, df):
        # type: (pd.DataFrame) -> Dict[str, Dict]
        stats_column_names = self._get_column_names_from_request(
            df, self.meta_conf.log_stats
        )
        df = df.filter(stats_column_names)
        try:
            stats = df.describe(include="all").to_json()
        except Exception as e:
            logger.warning("Failed to describe df: %s", e)
            stats = df.explode().describe(include="all").to_json()
        stats = json.loads(stats)
        stats = self._remove_none_values(stats)
        columns_stats: List[ColumnStatsArgs] = []
        for column_name in stats.keys():
            # Calculate ColumnStatsArgs metrics
            records_count = df[column_name].size
            null_count = np.count_nonzero(pd.isnull(df[column_name]))
            try:
                distinct_count = len(df[column_name].unique())
            except Exception:
                logger.warning("Failed to determine column type for: %s.", column_name)
                try:
                    distinct_count = len(df[column_name].astype("str").unique())
                except Exception:
                    # Support pandas >= v1.0
                    distinct_count = len(df[column_name].astype("string").unique())

            column_type = self._get_column_type(df[column_name])
            column_stats = ColumnStatsArgs(
                column_name=column_name,
                column_type=column_type,
                records_count=records_count,
                null_count=null_count,
                distinct_count=distinct_count,
                most_freq_value=stats[column_name].get("top"),
                most_freq_value_count=stats[column_name].get("freq"),
                unique_count=stats[column_name].get("unique"),
                mean_value=stats[column_name].get("mean"),
                min_value=stats[column_name].get("min"),
                max_value=stats[column_name].get("max"),
                std_value=stats[column_name].get("std"),
                quartile_1=stats[column_name].get("25%"),
                quartile_2=stats[column_name].get("50%"),
                quartile_3=stats[column_name].get("75%"),
            )
            columns_stats.append(column_stats)

        return columns_stats

    def _remove_none_values(self, input_dict):
        """remove none values from dict recursively"""
        for key, value in list(input_dict.items()):
            if value is None:
                input_dict.pop(key)
            elif isinstance(value, dict):
                self._remove_none_values(value)
        return input_dict

    def _get_column_type(self, column):
        # type: (pd.Series) -> str
        first_index = column.first_valid_index()
        if first_index is None:
            return column.dtype.name
        first_value = column.at[first_index]
        return type(first_value).__name__

    def _calculate_histograms(self, df_column, columns_stats):
        # type: (pd.Series, List[ColumnStatsArgs]) -> Optional[Tuple[List, List]]
        try:
            if len(df_column) == 0:
                return

            column_type = self._get_column_type(df_column)
            if "1" < pd.__version__ < "1.2":
                # handle  'Float32', 'Float64', 'Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64'
                column_type = column_type.lower()
            if is_string_dtype(column_type) or is_bool_dtype(column_type):
                counts = df_column.value_counts()  # type: pd.Series
                column_stats = get_column_stats_by_col_name(
                    columns_stats, df_column.name
                )
                if column_stats and column_stats.null_count:
                    null_column = pd.Series([column_stats.null_count], index=[None])
                    counts = counts.append(null_column)
                    counts = counts.sort_values(ascending=False)
                if len(counts) > 50:
                    counts, tail = counts[:49], counts[49:]
                    tail_sum = pd.Series([tail.sum()], index=["_others"])
                    counts = counts.append(tail_sum)
                values = counts.index
            elif is_numeric_dtype(column_type):
                df_column = df_column.dropna()
                counts, values = np.histogram(
                    df_column, bins=20
                )  # Tuple[np.array, np.array]
            else:
                return

            return counts.tolist(), values.tolist()
        except Exception:
            logger.exception(
                "log_histogram: Something went wrong for column '%s'", df_column.name
            )
