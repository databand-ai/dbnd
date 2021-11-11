from dbnd import PythonTask, output, parameter, task


class TestDocRunATaskInPython:
    def test_doc(self):
        #### DOC START
        @task
        def calculate_alpha(alpha=0.5):
            return alpha

        class CalculateBeta(PythonTask):
            beta = parameter.value(0.1)
            result = output

            def run(self):
                self.result.write(self.beta)

        # if __name__ == "__main__":
        calculate_alpha.dbnd_run(alpha=0.4)
        CalculateBeta(beta=0.15).dbnd_run()
        #### DOC END
