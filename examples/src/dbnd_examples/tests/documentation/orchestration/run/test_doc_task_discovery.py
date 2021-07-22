#### TESTED

from pytest import raises

from dbnd import PythonTask, parameter, task


class PrepareData(PythonTask):
    data = parameter.value("default")

    def run(self):
        return self.data


class TestDocTaskDiscovery:
    def test_doc(self):
        #### DOC START
        prepared_data = PrepareData(data="just some data")
        print(prepared_data)  # --> "prepare_data__19d1172be0"

        print(prepared_data.task_namespace)  # --> ""
        print(prepared_data.task_family)  # --> "prepare_data"
        print(prepared_data.task_id)  # --> "prepare_data__19d1172be0"

        print(PrepareData.task_namespace)  # --> ""
        print(PrepareData.task_family)  # --> "<property object at 0x1111e9548>"
        with raises(AttributeError):
            PrepareData.task_id  # --> Error!
        #### DOC END
