import json
import logging
import typing

import numpy as np
import pandas as pd

from pandas.core.dtypes.common import is_bool_dtype, is_numeric_dtype, is_string_dtype


if typing.TYPE_CHECKING:
    from typing import Tuple, Dict, Optional, List
    from targets.value_meta import ValueMetaConf
    from dbnd._core.tracking.log_data_request import LogDataRequest

logger = logging.getLogger(__name__)


class PandasHistograms(object):
    """
    calculates histograms and stats on pandas dataframe.
    """

    def __init__(self, df, meta_conf):
        # type: (pd.DataFrame, ValueMetaConf) -> None
        self.df = df
        self.meta_conf = meta_conf

    def get_histograms_and_stats(self):
        # type: () -> Tuple[Dict[str, Dict], Dict[str, List[List]]]
        stats, histograms = dict(), dict()
        if self.meta_conf.log_stats:
            stats = self._calculate_stats(self.df)

        if self.meta_conf.log_histograms:
            hist_column_names = self._get_column_names_from_request(
                self.df, self.meta_conf.log_histograms
            )
            df_histograms = self.df.filter(hist_column_names)
            if pd.__version__ > "1":
                # return_type="expand" and orient="list" are used to stabilize produced
                # histograms data structure across pandas v0 & v1
                histograms = df_histograms.apply(
                    self._calculate_histograms, result_type="expand", args=(stats,)
                ).to_dict(orient="list")
            else:
                histograms = df_histograms.apply(
                    self._calculate_histograms, args=(stats,)
                ).to_dict()
                histograms = {k: list(v) for k, v in histograms.items()}

        return stats, histograms

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
        stats = df.describe(include="all").to_json()
        stats = json.loads(stats)
        stats = self._remove_none_values(stats)
        for col in stats.keys():
            stats[col]["null-count"] = np.count_nonzero(pd.isnull(df[col]))
            stats[col]["count"] = df[col].size
            stats[col]["non-null"] = stats[col]["count"] - stats[col]["null-count"]
            stats[col]["distinct"] = len(df[col].unique())
            stats[col]["type"] = self._get_column_type(df[col])
        return stats

    def _remove_none_values(self, input_dict):
        """ remove none values from dict recursively """
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
        first_value = column.iat[first_index]
        return type(first_value).__name__

    def _calculate_histograms(self, df_column, stats):
        # type: (pd.Series, Dict) -> Optional[Tuple[List, List]]
        try:
            if len(df_column) == 0:
                return

            column_type = self._get_column_type(df_column)
            if is_bool_dtype(column_type) or is_string_dtype(column_type):
                counts = df_column.value_counts()  # type: pd.Series
                if (
                    stats
                    and df_column.name in stats
                    and "null-count" in stats[df_column.name]
                ):
                    null_count = stats[df_column.name]["null-count"]
                    null_column = pd.Series([null_count], index=[None])
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
                )  # type: np.array, np.array
            else:
                return

            return counts.tolist(), values.tolist()
        except Exception:
            logger.exception(
                "log_histogram: Something went wrong for column '%s'", df_column.name
            )
