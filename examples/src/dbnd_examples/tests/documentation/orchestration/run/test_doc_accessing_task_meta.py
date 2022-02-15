from pytest import raises

from dbnd import PythonTask, parameter


class PrepareData(PythonTask):
    data = parameter.value("default")

    def run(self):
        return self.data


class TestDocAccessingTaskMeta:
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

    def test_current_task_object(self):
        #### DOC START
        from dbnd import current_task, task

        @task
        def calculate_alpha(alpha: int = 0.5):
            return current_task().task_version

        #### DOC END
        calculate_alpha.dbnd_run()

    def test_current_env_inside_task_or_pipeline(self):
        #### DOC START
        from dbnd import current_task, task

        @task
        def calculate_alpha(alpha: int = 0.5):
            return current_task().task_env.name  # The environment of the task
            # See EnvConfig object for all properties

        #### DOC END
        calculate_alpha.dbnd_run()
