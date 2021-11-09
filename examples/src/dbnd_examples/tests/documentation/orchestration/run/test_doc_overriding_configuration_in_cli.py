import time

from dbnd import pipeline, task


class TestDocOverridingConfigurationInCLI:
    def test_doc(self):
        #### DOC START
        @task
        def calculate_alpha(alpha=0.5):
            return alpha

        @pipeline
        def train_model_pipeline():
            calculate_alpha(0.1)

        #### DOC END

        train_model_pipeline.dbnd_run(task_version=time.time())
