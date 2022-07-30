# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from typing import Iterable, Optional

import six

from dbnd._core.configuration.config_store import _ConfigStore, _lower_config_name
from dbnd._core.utils.basics.text_banner import TextBanner, safe_string, safe_tabulate


if typing.TYPE_CHECKING:
    from dbnd._core.configuration.dbnd_config import DbndConfig

logger = logging.getLogger(__name__)


def pformat_current_config(config, as_table=False, sections=None):
    # type: (DbndConfig, bool, Optional[Iterable[str]]) -> str
    config_layer = config.config_layer
    tb = TextBanner("Config {}".format(config_layer.name))
    tb.column("LAYERS", config_layer.config_layer_path)
    if as_table:
        view_str = pformat_config_store_as_table(
            config_store=config_layer.config, sections=sections
        )
    else:
        value_dict = config_layer.config.as_value_dict(sections=sections)
        view_str = tb.f_struct(value_dict)

    tb.column("CONFIG", view_str)
    return tb.get_banner_str()


def pformat_config_store_as_table(config_store, sections=None):
    # type: (_ConfigStore, Optional[Iterable[str]]) -> str
    header = ["Section", "Key", "Value", "Source", "Priority"]
    data = []
    if sections:
        sections = [_lower_config_name(s) for s in sections]
    else:
        sections = config_store.keys()

    for section in sections:
        section_values = config_store.get(section)
        if not section_values:
            continue
        for key, config_value in six.iteritems(section_values):
            data.append(
                (
                    section,
                    key,
                    safe_string(config_value.value, 300),
                    config_value.source,
                    config_value.priority,
                )
            )

    if data:
        return safe_tabulate(tabular_data=data, headers=header)
    return ""


def pformat_all_layers(config, sections=None):
    # type: (DbndConfig, Optional[Iterable[str]]) -> str
    tb = TextBanner("Configs")

    layers = config.config_layer.get_all_layers()

    # start from the first one
    for layer in reversed(layers):
        layer_values = layer.layer_config.as_value_dict(sections=sections)
        tb.column("LAYER {}".format(layer.config_layer_path), tb.f_struct(layer_values))
    return tb.get_banner_str()
