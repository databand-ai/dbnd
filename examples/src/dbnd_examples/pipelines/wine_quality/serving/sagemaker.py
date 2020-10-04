# ORIGIN: https://github.com/databricks/mlflow
import json
import logging
import os

import boto3

from dbnd import PythonTask, current as current_task_context, output, parameter, task
from dbnd.errors import DatabandError
from dbnd_examples.pipelines.wine_quality.serving.aws import (
    DeployMode,
    SageMakerManager,
    SageMakerOperationStatus,
    get_assumed_role_arn,
    get_default_s3_bucket,
    upload_s3,
)
from dbnd_examples.pipelines.wine_quality.serving.utils import run_commands
from targets.types import PathStr


_full_template = "{account}.dkr.ecr.{region}.amazonaws.com/{image}:{version}"


@task
def push_image_to_ecr(image, name):
    # type: (str, str) -> str
    logging.info("Pushing image to ECR")
    image = image.split(":")[0]
    version = "{}-{}".format(name, current_task_context.task_version)

    client = boto3.client("sts")
    caller_id = client.get_caller_identity()
    account = caller_id["Account"]
    my_session = boto3.session.Session()
    region = my_session.region_name or "us-east-1"
    fullname = _full_template.format(
        account=account, region=region, image=image, version=version
    )
    logging.info(
        "Pushing docker image {image} to {repo}".format(image=image, repo=fullname)
    )

    ecr_client = boto3.client("ecr")

    try:
        ecr_client.describe_repositories(repositoryNames=[image])["repositories"]
    except ecr_client.exceptions.RepositoryNotFoundException:
        ecr_client.create_repository(repositoryName=image)
        print(
            "Created new ECR repository: {repository_name}".format(
                repository_name=image
            )
        )

    # TODO: it would be nice to translate the docker login, tag and push to python api.
    # x = ecr_client.get_authorization_token()['authorizationData'][0]
    # docker_login_cmd = "docker login -u AWS -p {token} {url}".format(token=x['authorizationToken']
    #                                                                ,url=x['proxyEndpoint'])

    docker_login_cmd = "$(aws ecr get-login --no-include-email)"
    os.system(docker_login_cmd)
    docker_tag_cmd = "docker tag {image} {fullname}".format(
        image=image, fullname=fullname
    )
    docker_push_cmd = "docker push {}".format(fullname)
    # cmd = ";\n".join([docker_login_cmd, docker_tag_cmd, docker_push_cmd])
    commands = [docker_tag_cmd, docker_push_cmd]
    run_commands(commands)

    return fullname


@task
def test_sagemaker_endpoint(endpoint_name, region="us-east-1"):
    # type: (str, str) -> str

    content_type = "application/json; format=pandas-split"
    data = {
        "columns": [
            "alcohol",
            "chlorides",
            "citric acid",
            "density",
            "fixed acidity",
            "free sulfur dioxide",
            "pH",
            "residual sugar",
            "sulphates",
            "total sulfur dioxide",
            "volatile acidity",
        ],
        "data": [[12.8, 0.029, 0.48, 0.98, 6.2, 29, 3.33, 1.2, 0.39, 75, 0.66]],
    }

    client = boto3.session.Session().client("sagemaker-runtime", region)

    response = client.invoke_endpoint(
        EndpointName=endpoint_name, Body=json.dumps(data), ContentType=content_type
    )
    prediction = response["Body"].read().decode("ascii")
    prediction = json.loads(prediction)
    assert 4 < prediction[0] < 8
    return str(prediction)[0]


def deploy_to_sagemaker(app_name, image_url, model_path):
    return SageMakerDeployTask(
        app_name=app_name, image_url=image_url, model_path=model_path
    )


class SageMakerDeployTask(PythonTask):
    model_path = parameter(description="Path to the model")[PathStr]
    app_name = parameter(description="Name of the deployed application.")[str]
    image_url = parameter(description="Name of the Docker image to be used.")[str]

    bucket = parameter.c(
        default=None,
        description="S3 bucket where model artifacts will be stored. Defaults to a "
        "SageMaker-compatible bucket name.",
    )[str]
    region_name = parameter.c(default="us-east-1")
    execution_role_arn = parameter.c(default=None)[str]

    instance_type = parameter.c(default="ml.m4.xlarge")
    instance_count = parameter.c(default=1)

    vpc_config = parameter.c(default=None)[str]

    mode = parameter.c(
        default=DeployMode.REPLACE, description="Sagemaker deployment mode"
    ).choices(DeployMode.ALL)
    archive = parameter.c(default=False)[bool]

    timeout_seconds = parameter.c(default=1200)
    synchronous = parameter[bool].c(default=True)

    result = output.csv

    def run(self):
        sage_client = boto3.client("sagemaker", region_name=self.region_name)
        s3_client = boto3.client("s3", region_name=self.region_name)
        sage_manager = SageMakerManager(sage_client)

        endpoint_exists = (
            sage_manager.find_endpoint(endpoint_name=self.app_name) is not None
        )
        if endpoint_exists and self.mode == DeployMode.CREATE:
            raise DatabandError(
                help_msg=(
                    "You are attempting to deploy an application with name: {application_name} in"
                    " '{mode_create}' mode. However, an application with the same name already"
                    " exists. If you want to update this application, deploy in '{mode_add}' or"
                    " '{mode_replace}' mode.".format(
                        application_name=self.app_name,
                        mode_create=DeployMode.CREATE,
                        mode_add=DeployMode.ADD,
                        mode_replace=DeployMode.REPLACE,
                    )
                )
            )

        s3_bucket_prefix = self.model_path
        if not self.execution_role_arn:
            self.execution_role_arn = get_assumed_role_arn()
        if not self.bucket:
            logging.info("No model data bucket specified, using the default bucket")
            self.bucket = get_default_s3_bucket(self.region_name)

        model_s3_path = upload_s3(
            local_model_path=self.model_path,
            bucket=self.bucket,
            prefix=s3_bucket_prefix,
            region_name=self.region_name,
            s3_client=s3_client,
        )

        if endpoint_exists and self.mode in [DeployMode.REPLACE, DeployMode.ADD]:
            deployment_operation = sage_manager.update_sagemaker_endpoint(
                endpoint_name=self.app_name,
                image_url=self.image_url,
                model_s3_path=model_s3_path,
                instance_type=self.instance_type,
                instance_count=self.instance_count,
                vpc_config=self.vpc_config,
                mode=self.mode,
                role=self.execution_role_arn,
                s3_client=s3_client,
            )
        else:
            deployment_operation = sage_manager.create_sagemaker_endpoint(
                endpoint_name=self.app_name,
                image_url=self.image_url,
                model_s3_path=model_s3_path,
                instance_type=self.instance_type,
                instance_count=self.instance_count,
                vpc_config=self.vpc_config,
                role=self.execution_role_arn,
            )

        if self.synchronous:
            logging.info("Waiting for the deployment operation to complete...")
            operation_status = deployment_operation.await_completion(
                timeout_seconds=self.timeout_seconds
            )
            if operation_status.state == SageMakerOperationStatus.STATE_SUCCEEDED:
                logging.info(
                    'The deployment operation completed successfully with message: "%s"',
                    operation_status.message,
                )
            else:
                raise DatabandError(
                    "The deployment operation failed with the following error message:"
                    ' "{error_message}"'.format(error_message=operation_status.message)
                )
            if not self.archive:
                deployment_operation.clean_up()

        self.result.write("blabls")
