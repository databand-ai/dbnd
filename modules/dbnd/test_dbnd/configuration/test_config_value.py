# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd._core.configuration.config_value import (
    ConfigValue,
    ConfigValuePriority,
    fold_config_value,
)


@pytest.mark.parametrize(
    "stack, lower, expected",
    [
        pytest.param([None], None, [None], id="All None"),
        pytest.param(
            [None],
            ConfigValue(value=10, source=None),
            [ConfigValue(value=10, source=None)],
            id="Nothing in lower level",
        ),
        pytest.param(
            [ConfigValue(value=10, source=None)],
            None,
            [ConfigValue(value=10, source=None)],
            id="First config value to find",
        ),
        pytest.param(
            [ConfigValue(value=10, source=None)],
            ConfigValue(value=11, source=None),
            [ConfigValue(value=10, source=None)],
            id="Same normal priority",
        ),
        pytest.param(
            [ConfigValue(value=10, source=None, priority=ConfigValuePriority.OVERRIDE)],
            ConfigValue(value=11, source=None),
            [ConfigValue(value=10, source=None, priority=ConfigValuePriority.OVERRIDE)],
            id="Higher has bigger priority",
        ),
        pytest.param(
            [ConfigValue(value=11, source=None)],
            ConfigValue(value=10, source=None, priority=ConfigValuePriority.OVERRIDE),
            [ConfigValue(value=10, source=None, priority=ConfigValuePriority.OVERRIDE)],
            id="Lower has bigger priority",
        ),
        pytest.param(
            [ConfigValue(value=[11], extend=True)],
            ConfigValue(value=[12]),
            [ConfigValue(value=[12]), ConfigValue(value=[11], extend=True)],
            id="List merge with no merge after it",
        ),
        pytest.param(
            [ConfigValue(value=[11], extend=True)],
            ConfigValue(value=[12], extend=True),
            [
                ConfigValue(value=[12], extend=True),
                ConfigValue(value=[11], extend=True),
            ],
            id="merge is followed by the lower level behaviour",
        ),
        pytest.param(
            [
                ConfigValue(value=[12], extend=True),
                ConfigValue(value=[11], extend=True),
            ],
            ConfigValue(value=[13]),
            [
                ConfigValue(value=[13]),
                ConfigValue(value=[12], extend=True),
                ConfigValue(value=[11], extend=True),
            ],
            id="Top of the stack allowing extend ",
        ),
        pytest.param(
            [ConfigValue(value=[12]), ConfigValue(value=[11], extend=True)],
            ConfigValue(value=[13]),
            [ConfigValue(value=[12]), ConfigValue(value=[11], extend=True)],
            id="Top of the stack not allowing and the values have the same priority",
        ),
        pytest.param(
            [ConfigValue(value=[11], extend=True, priority=ConfigValuePriority.NORMAL)],
            ConfigValue(value=[12], priority=ConfigValuePriority.OVERRIDE),
            [ConfigValue(value=[12], priority=ConfigValuePriority.OVERRIDE)],
            id="Merge Normal into override is not working",
        ),
        pytest.param(
            [
                ConfigValue(
                    value={"a": 11}, extend=True, priority=ConfigValuePriority.OVERRIDE
                )
            ],
            ConfigValue(value={"b": 12}),
            [
                ConfigValue(value={"b": 12}),
                ConfigValue(
                    value={"a": 11}, extend=True, priority=ConfigValuePriority.OVERRIDE
                ),
            ],
            id="Merge Override into normal gets normal priority",
        ),
        pytest.param(
            [
                ConfigValue(
                    value=[11], extend=True, priority=ConfigValuePriority.OVERRIDE
                )
            ],
            ConfigValue(value=[12], extend=True, priority=ConfigValuePriority.OVERRIDE),
            [
                ConfigValue(
                    value=[12], extend=True, priority=ConfigValuePriority.OVERRIDE
                ),
                ConfigValue(
                    value=[11], extend=True, priority=ConfigValuePriority.OVERRIDE
                ),
            ],
            id="Two overrides can be merged",
        ),
        pytest.param(
            [ConfigValue(value=[11], extend=True, priority=ConfigValuePriority.NORMAL)],
            ConfigValue(value=[12], extend=True, priority=ConfigValuePriority.FALLBACK),
            [
                ConfigValue(
                    value=[12], extend=True, priority=ConfigValuePriority.FALLBACK
                ),
                ConfigValue(
                    value=[11], extend=True, priority=ConfigValuePriority.NORMAL
                ),
            ],
            id="Merging normal into fallback",
        ),
        pytest.param(
            [ConfigValue(value=[11], extend=True, priority=ConfigValuePriority.NORMAL)],
            ConfigValue(value=[12], extend=True, priority=ConfigValuePriority.FALLBACK),
            [
                ConfigValue(
                    value=[12], extend=True, priority=ConfigValuePriority.FALLBACK
                ),
                ConfigValue(
                    value=[11], extend=True, priority=ConfigValuePriority.NORMAL
                ),
            ],
            id="Merging normal into fallback",
        ),
        pytest.param(
            [
                ConfigValue(
                    value=[12], extend=True, priority=ConfigValuePriority.NORMAL
                ),
                ConfigValue(
                    value=[11], extend=True, priority=ConfigValuePriority.OVERRIDE
                ),
            ],
            ConfigValue(value=[13], priority=ConfigValuePriority.NORMAL),
            [
                ConfigValue(value=[13], priority=ConfigValuePriority.NORMAL),
                ConfigValue(
                    value=[12], extend=True, priority=ConfigValuePriority.NORMAL
                ),
                ConfigValue(
                    value=[11], extend=True, priority=ConfigValuePriority.OVERRIDE
                ),
            ],
            id="priority is checked only for the top of the stack",
        ),
        pytest.param(
            [
                ConfigValue(
                    value={"b": 12}, extend=True, priority=ConfigValuePriority.NORMAL
                ),
                ConfigValue(
                    value={"a": 11}, extend=True, priority=ConfigValuePriority.OVERRIDE
                ),
            ],
            ConfigValue(value=[13], priority=ConfigValuePriority.OVERRIDE),
            [ConfigValue(value=[13], priority=ConfigValuePriority.OVERRIDE)],
            id="Top of the stack allowing extend but the lower value has higher priority",
        ),
    ],
)
def test_fold_config_value_with_prev(stack, lower, expected):
    assert fold_config_value(stack, lower) == expected
