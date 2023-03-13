# © Copyright Databand.ai, an IBM Company 2022

from unittest import TestCase

from dbnd_airflow.export_plugin.helpers import (
    MAX_RECURSIVE_CALL_NUM,
    _extract_args_from_dict,
)


NESTED_KEY = "cyclic_key"


class TestHelpers(TestCase):
    def test_cyclic_args_import(self):
        cyclic_args_dict = {"owner": "airflow"}

        # Add a key-value pair to the dictionary
        cyclic_args_dict[NESTED_KEY] = {}

        # Add a key-value pair to the nested dictionary
        cyclic_args_dict[NESTED_KEY] = cyclic_args_dict

        result = _extract_args_from_dict(cyclic_args_dict)

        assert self._get_recursive_key_value(result, NESTED_KEY, MAX_RECURSIVE_CALL_NUM)

        with self.assertRaises(KeyError):
            self._get_recursive_key_value(
                result, NESTED_KEY, MAX_RECURSIVE_CALL_NUM + 1
            )

    def _get_recursive_key_value(self, dct, key, nested_level):
        for i in range(0, nested_level):
            dct = dct[key]
        return dct
