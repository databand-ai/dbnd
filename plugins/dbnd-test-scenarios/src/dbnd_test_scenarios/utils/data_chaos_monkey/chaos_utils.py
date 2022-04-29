import math

from random import randint, random

import numpy as np
import pandas as pd


def chaos_int(value):
    value = int(value)
    return value + randint(math.ceil(-0.1 * value), math.ceil(0.1 * value))


def chaos_float(value):
    value = float(value)
    return value * (0.9 + 0.2 * random())


def create_nulls(
    df: pd.DataFrame, column: str, percentage_of_null: int = 90
) -> pd.DataFrame:
    """
    This function gets a pandas DF, and sabotage in it by changing some values to null in a specific column

    :param df: Dataframe to sabotage in
    :param column: Column name to create nulls in
    :param percentage_of_null: Percentage of the column to fulfil with nulls instead of real values. Default: 90%
    :return: Sabotaged DF
    """
    number_of_nulls = int((percentage_of_null / 100) * len(df.index))
    df[column][:number_of_nulls] = df[column][:number_of_nulls].replace(
        to_replace=".*", value=np.nan, regex=True
    )
    return df


if __name__ == "__main__":
    print(chaos_int(200), chaos_int(0.5), chaos_int(15))
    print(chaos_float(200), chaos_float(0.5), chaos_float(15))
