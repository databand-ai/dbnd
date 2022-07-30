# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict

import attr

from dbnd import band, task
from targets.values import ValueType, register_value_type


logger = logging.getLogger(__name__)


###############
# USER PART
@attr.s
class MyConfig(object):
    my_config_id = attr.ib()
    data = attr.ib()


#######################
# DATABAND INTEGRATION
class MyConfigValueType(ValueType):
    type = MyConfig

    def parse_from_str(self, x):
        # calculate config
        logger.warning("Calculating my_config with id %s", x)
        return MyConfig(my_config_id=x, data={"config": "1"})


register_value_type(MyConfigValueType())


###############
# USAGE


@task
def update_config(my_config=1):
    # type: (MyConfig)-> MyConfig
    new_config = MyConfig(
        my_config_id="{}_patched".format(my_config.my_config_id), data={"config": "2"}
    )

    return new_config


@task
def use_config(my_config):
    # type: (MyConfig)-> Dict
    assert my_config.data["config"] == "2"
    return my_config.data


@band(result=("updated", "config_data"))
def config_flow(my_config):
    # type: (MyConfig)->  (MyConfig, Dict)
    updated_config = update_config(my_config)
    config_data = use_config(updated_config)
    return updated_config, config_data
