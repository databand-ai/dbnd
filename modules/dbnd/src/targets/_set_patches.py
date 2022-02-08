import logging


logger = logging.getLogger(__name__)


def target_to_databand(df, target, **kwargs):
    """Targets to databand."""
    return target.as_pandas.to(df, **kwargs)


try:
    from pandas import DataFrame

    DataFrame.to_target = target_to_databand
except ImportError:
    logger.warning(
        "'pandas' library can not be imported. This will crash at runtime if Pandas functionality is used."
    )

# TODO: Dask
# try:
#     from dask.dataframe import DataFrame
#     DataFrame.to_target = target_to_databand
# except ImportError:
#     logger.warning(
#         "'pandas' library can not be imported. This will crash at runtime if Pandas functionality is used."
#     )
