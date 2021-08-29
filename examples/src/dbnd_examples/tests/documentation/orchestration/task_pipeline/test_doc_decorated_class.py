from dbnd import task
from dbnd_examples.data import data_repo


# place to store all examples that decorated class


class TestDocDecoratedClass:
    def test_prepare_data_decorated_class(self):
        #### DOC START
        @task
        class PrepareData(object):
            def __init__(self, data):
                self.data = data

            def run(self):
                return self.data

        #### DOC END
        preparedData = PrepareData(data_repo.wines)
        preparedData.run()
