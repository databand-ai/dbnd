#### TESTED
from dbnd import PythonTask, parameter


class TestDocInstanceCaching:
    def test_doc(self):
        #### DOC START
        class calculate_alpha(PythonTask):
            alpha = parameter[int]

        a = (0.5,)
        b = (0.5,)

        assert a is not b

        c = calculate_alpha(alpha=a)
        d = calculate_alpha(alpha=b)

        assert c is d
        #### DOC END
