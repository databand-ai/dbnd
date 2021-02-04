from typing import Dict, List

import pandas as pd

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
