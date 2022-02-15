from pandas import DataFrame


#### DOC START
# module1.py
def calculate_alpha():
    pass


def calculate_beta():
    pass


def calculate_gamma():
    pass


#### DOC END


class TestDocFunctions:
    def test_tracking_functions_with_decorator(self):
        #### DOC START
        # module1.py
        from dbnd import task

        # define a function with a decorator

        @task
        def prepare_data(data: DataFrame, counter: int, random: int):
            return "OK"

        #### DOC END

    def test_tracking_named_functions(self):
        #### DOC START
        # module2.py

        from dbnd import track_functions

        # from module1 import calculate_alpha, calculate_beta, calculate_gamma

        track_functions(calculate_alpha, calculate_beta, calculate_gamma)

        def calculate_coefficient():
            calculate_alpha()
            calculate_beta()
            calculate_gamma()

        # module3.py

        """from dbnd import track_module_functions
        import module1
        import module2
        from module2 import calculate_coefficient

        track_modules(module1, module2)"""

        def calculate_all():
            calculate_coefficient()

        #### DOC END
        calculate_all()

    def test_alternate_calculate_coefficient(self):
        module1 = None

        #### DOC START
        # module2.py

        from dbnd import track_module_functions

        # from module1 import calculate_alpha, calculate_beta, calculate_gamma
        # import module1

        track_module_functions(module1)

        def calculate_coefficient():
            calculate_alpha()
            calculate_beta()
            calculate_gamma()

        #### DOC END
        calculate_coefficient()
