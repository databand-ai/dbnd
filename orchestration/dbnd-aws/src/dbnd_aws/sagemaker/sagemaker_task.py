# © Copyright Databand.ai, an IBM Company 2022

from dbnd import output, parameter
from dbnd._core.task.task import Task
from dbnd_aws.sagemaker.estimator_config import BaseEstimatorConfig
from dbnd_aws.sagemaker.sagemaker_ctrl import SageMakerCtrl
from targets import Target


class SageMakerTrainTask(Task):
    estimator_config = parameter[BaseEstimatorConfig]

    train = parameter(description="path to an s3 bucket where training data is stored")[
        Target
    ]
    test = parameter.none().help("path to an s3 bucket where test data is stored")[
        Target
    ]
    validate = parameter.none().help(
        "path to an s3 bucket where validation data is stored"
    )[Target]

    region = parameter(
        default="us-east-1", description="region to use for docker image resolution"
    )
    output_path = (
        output.description("s3 path to output a model")
        .folder_data.with_flag(None)
        .no_extension[Target]
    )
    sagemaker_role = parameter[str]

    wait_for_completion = parameter.c(default=True)[bool]
    print_log = parameter.c(default=True)[bool]
    check_interval = parameter.c(default=30)[int]
    max_ingestion_time = parameter.c(default=None)[int]

    def _get_ctrl(self):
        # type: ()-> SageMakerCtrl
        return SageMakerCtrl(self)

    def _task_submit(self):
        self._get_ctrl().train()
        return "Ok"
