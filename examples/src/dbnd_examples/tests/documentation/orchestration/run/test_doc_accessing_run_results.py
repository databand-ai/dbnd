# © Copyright Databand.ai, an IBM Company 2022

from datetime import datetime

from pandas import DataFrame
from sklearn.linear_model import ElasticNet

from dbnd import pipeline
from dbnd_examples.data import data_repo
from dbnd_examples.tests.documentation.orchestration.test_quick_start_wine_quality import (
    prepare_data,
    train_model,
    validate_model,
)


class TestDocAccessingRunResults:
    def test_doc(self):
        #### DOC START
        @pipeline(result=("model", "validation"))
        def predict_wine_quality(
            raw_data: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5
        ):
            training_set, validation_set = prepare_data(raw_data=raw_data)
            model = train_model(
                training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
            )

            validation = validate_model(model=model, validation_dataset=validation_set)

            return model, validation

        run = predict_wine_quality.dbnd_run(
            raw_data=data_repo.wines, task_version=datetime.now()
        )

        model = run.load_from_result("model", value_type=ElasticNet)
        validation = run.load_from_result("validation")
        #### DOC END
        assert model and validation
