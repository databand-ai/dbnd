# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict, List

import mock
import pandas as pd

from mock import patch

from dbnd._core.utils.typing_utils.doc_annotations import get_doc_annotaions


class TestDocAnnotationParsing(object):
    def test_simple_parse(self):
        def f(a):
            # type: (int)->str
            return "ok"

        actual = get_doc_annotaions(f, ["a"])
        assert actual["a"] == "int"
        assert actual["return"] == "str"

    def test_nested_parse1(self):
        def f(a, b):
            # type: (Dict[str, Dict[str, pd.DataFrame]], List[int])->str
            return "ok"

        actual = get_doc_annotaions(f, ["a", "b"])
        assert actual["a"] == "Dict[str,Dict[str,pd.DataFrame]]"
        assert actual["b"] == "List[int]"
        assert actual["return"] == "str"

    def test_nested_parse2(self):
        def f(a):
            # type: ( Dict[str, Dict[str, pd.DataFrame]]) -> str
            return "ok"

        actual = get_doc_annotaions(f, ["a", "b"])
        assert actual["a"] == "Dict[str,Dict[str,pd.DataFrame]]"
        assert actual["return"] == "str"

    @patch(
        "inspect.getsource",
        mock.MagicMock(side_effect=OSError("could not get source code")),
    )
    def test_ipython_parse(self):
        def f(a):
            # type: (int)->str
            return "ok"

        actual = get_doc_annotaions(f, ["a"])
        assert actual is None
