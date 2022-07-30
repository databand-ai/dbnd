# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict, Type

import luigi

from dbnd import parameter
from dbnd_luigi import luigi_parameters


logger = logging.getLogger(__name__)


def get_dbnd_param_by_luigi_name(luigi_param_name):
    """
    Returns the dbnd parameter which is mapped to the luigi_param_name if exists, returns None otherwise.
    """
    parameter_store = luigi_parameters.__dict__
    return parameter_store.get(luigi_param_name, None)


def _build_dbnd_parameter(luigi_param):
    """
    Creates dbnd parameter which match to the luigi param
    """
    matching_dbnd_parameter = get_dbnd_param_by_luigi_name(
        luigi_param.__class__.__name__
    )
    if not matching_dbnd_parameter:
        logger.warning(
            "Could not convert luigi parameter {0} to dbnd parameter!".format(
                luigi_param.__class__.__name__
            )
        )
        return None

    # Instantiate new object to prevent overwriting
    matching_dbnd_parameter = matching_dbnd_parameter(
        default=luigi_param._default, description=luigi_param.description
    )

    return matching_dbnd_parameter


def extract_luigi_params(luigi_task_cls):
    # type: ( Type[luigi.Task])-> Dict[str,  parameter]
    """
    return the luigi params wrapped as dbnd params
    """
    luigi_params = luigi_task_cls.get_params()
    return {
        param_name: _build_dbnd_parameter(param_obj)
        for param_name, param_obj in luigi_params
    }
