# © Copyright Databand.ai, an IBM Company 2022

from dbnd import pipeline
from dbnd_aws.sagemaker import SageMakerTrainTask
from dbnd_aws.sagemaker.estimator_config import PyTorchEstimatorConfig


@pipeline
def train_mnist_with_pytorch(train: str):
    hyperparameters = {"epochs": 6, "backend": "gloo"}

    train = SageMakerTrainTask(
        train=train,
        estimator_config=PyTorchEstimatorConfig(
            entry_point="dbnd_examples_orchestration/tool_sagemaker/mnist.py",
            framework_version="1.1.0",
            hyperparameters=hyperparameters,
        ),
    )
    return train.output_path
