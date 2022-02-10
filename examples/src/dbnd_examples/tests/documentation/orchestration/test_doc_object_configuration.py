import time

from pandas import DataFrame
from sklearn.linear_model import ElasticNet

#### DOC START
from dbnd import task
from dbnd_examples.data import data_repo


@task
def train_model(
    training_set: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5
) -> ElasticNet:
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop(["quality"], 1), training_set[["quality"]])
    return lr


def calculate_alpha():
    return "alpha=%s" % (time.time(),)


#### DOC END


class TestDocObjectConfiguration:
    def test_doc(self):
        train_model.dbnd_run(
            training_set=data_repo.wines, task_version=calculate_alpha()
        )
