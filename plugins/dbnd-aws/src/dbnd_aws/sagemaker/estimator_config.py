from typing import Dict

from dbnd import Config, parameter


class Algorithm(object):
    PCA = "pca"
    KMEANS = "kmeans"
    LINEAR_LEARNER = "linear-learner"
    FACTORIZATION_MACHINES = "factorization-machines"
    NTM = "ntm"
    RANDOMCUTFOREST = "randomcutforest"
    KNN = "knn"
    OBJECT2VEC = "object2vec"
    IPUNSIGHTS = "ipinsights"
    LDA = "lda"
    FORCATSING_DEEPAR = "forecasting-deepar"
    XGBOOST = "xgboost"
    SEQ2SEQ = "seq2seq"
    IMAGE_CLASIFICATION = "image-classification"
    BLAZONGTEXT = "blazingtext"
    OBJECT_DETECTION = "object-detection"
    SEMANTIC_SEGMENTATION = "semantic-segmentation"
    IMAGE_CLASIFICATION_NEO = "image-classification-neo"
    XGBOOST_NEO = "xgboost-neo"


class BaseEstimatorConfig(Config):
    """AWS SageMaker (-s [TASK].estimator.[PARAM]=[VAL] for specific tasks)"""

    # we don't want spark class to inherit from this one, as it should has Config behaviour
    _conf__task_family = "estimator"

    train_instance_count = parameter(default=1)[int]
    train_instance_type = parameter(
        default="ml.c5.4xlarge", description="EC2 instance type to use for a training"
    )[str]
    train_volume_size = parameter(default=30)[int]
    train_max_run = parameter(default=3600)[int]
    base_job_name = parameter.c(default="trng-recommender")
    hyperparameters = parameter(
        empty_default=True, description="Hyperparameter tuning configuration"
    )[Dict]

    def get_input_dict(self, train, test, validate):
        raise NotImplemented("Subclass should implement")

    def get_estimator(self, task):
        raise NotImplemented("Subclass should implement")

    def get_estimator_ctrl(self):
        raise NotImplemented("Subclass should implement")


class GenericEstimatorConfig(BaseEstimatorConfig):
    _conf__task_family = "generic_estimator"
    algorithm = parameter(
        default=Algorithm.FACTORIZATION_MACHINES, description="algorithm name"
    )

    def get_input_dict(self, train, test, validate):
        inputs = dict()
        inputs["train"] = str(train)
        if test:
            inputs["test"] = str(test)
        if validate:
            inputs["validate"] = str(validate)
        return inputs

    def _to_estimator_conf(self, task):
        from sagemaker.amazon.amazon_estimator import get_image_uri

        return {
            "image_name": get_image_uri(task.region, task.estimator_config.algorithm),
            "role": task.sagemaker_role,
            "train_instance_count": task.estimator_config.train_instance_count,
            "train_instance_type": task.estimator_config.train_instance_type,
            "train_volume_size": task.estimator_config.train_volume_size,
            "output_path": str(task.output_path),
            "base_job_name": task.estimator_config.base_job_name,
            "hyperparameters": task.estimator_config.hyperparameters,
        }

    def get_estimator(self, task):
        from sagemaker.estimator import Estimator

        conf = self._to_estimator_conf(task)
        return Estimator(**conf)


class PyTorchEstimatorConfig(BaseEstimatorConfig):
    entry_point = parameter.description("Path to a source file")[str]
    source_dir = parameter.description("Path to a additional source files").none()[str]
    py_version = parameter(default="py3", description="Python version")[str]
    framework_version = parameter.description("PyTorch version").none[str]
    image_name = parameter.description("Custom Image").none[str]

    def get_input_dict(self, train, test, validate):
        inputs = dict()
        inputs["training"] = str(train)
        if test:
            inputs["test"] = str(test)
        if validate:
            inputs["validate"] = str(validate)
        return inputs

    def _to_estimator_conf(self, task):
        return {
            "entry_point": task.estimator_config.entry_point,
            "framework_version": task.estimator_config.framework_version,
            "image_name": task.estimator_config.image_name,
            "role": task.sagemaker_role,
            "train_instance_count": task.estimator_config.train_instance_count,
            "train_instance_type": task.estimator_config.train_instance_type,
            "train_volume_size": task.estimator_config.train_volume_size,
            "output_path": str(task.output_path),
            "hyperparameters": task.estimator_config.hyperparameters,
        }

    def get_estimator(self, task):
        from sagemaker.pytorch import PyTorch

        conf = self._to_estimator_conf(task)
        return PyTorch(**conf)
