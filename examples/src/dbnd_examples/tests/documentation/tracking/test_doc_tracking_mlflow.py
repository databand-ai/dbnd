# © Copyright Databand.ai, an IBM Company 2022

"""from random import random

class TestDocTrackingMlflow:
    def test_doc(self):
        #### DOC START
        from dbnd import task
        from mlflow import start_run, end_run
        from mlflow import log_metric, log_param

        @task
        def calculate_alpha():
            start_run()
            # params
            log_param("alpha", random())
            log_param("beta", random())
            # metrics
            log_metric("accuracy", random())
            log_metric("coefficient", random())
            end_run()

        #### DOC END
        calculate_alpha.dbnd_run()"""
