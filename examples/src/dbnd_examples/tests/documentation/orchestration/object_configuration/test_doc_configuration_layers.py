from dbnd import task


class TestDocConfigurationLayers:
    def test_doc(self):
        #### DOC START
        @task
        def calculate_alpha(alpha=0.5, beta=0.1):
            return alpha

        alpha_task = calculate_alpha(alpha=4)
        #### DOC END
