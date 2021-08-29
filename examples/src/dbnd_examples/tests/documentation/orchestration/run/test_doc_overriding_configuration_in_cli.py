from dbnd import task


class TestDocOverridingConfigurationInCLI:
    def test_doc(self):
        #### DOC START
        @task
        def calculate_alpha(alpha=0.5):
            return alpha

        @task
        def calculate_beta():
            calculate_alpha(0.1)

        #### DOC END

        calculate_beta.dbnd_run()
