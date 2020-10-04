# ORIGIN: https://github.com/databricks/mlflow
import logging
import os
import time
import urllib

import boto3

from dbnd._vendor.tmpdir import TempDir
from dbnd_examples.pipelines.wine_quality.serving.utils import (
    get_unique_resource_id,
    make_tarfile,
)


DEFAULT_BUCKET_NAME_PREFIX = "databand-sagemaker"
DEPLOYMENT_CONFIG_KEY_FLAVOR_NAME = "deployment_flavor_name"


class DeployMode(object):
    CREATE = "create"
    ADD = "add"
    REPLACE = "replace"
    ALL = [CREATE, ADD, REPLACE]


class SageMakerManager(object):
    def __init__(self, sage_client):
        self.client = sage_client

    def find_endpoint(self, endpoint_name):
        endpoints_page = self.client.list_endpoints(
            MaxResults=100, NameContains=endpoint_name
        )

        while True:
            for endpoint in endpoints_page["Endpoints"]:
                if endpoint["EndpointName"] == endpoint_name:
                    return endpoint

            if "NextToken" in endpoints_page:
                endpoints_page = self.client.list_endpoints(
                    MaxResults=100,
                    NextToken=endpoints_page["NextToken"],
                    NameContains=endpoint_name,
                )
            else:
                return None

    @staticmethod
    def _get_sagemaker_model_name(endpoint_name):
        return "{en}-model-{uid}".format(en=endpoint_name, uid=get_unique_resource_id())

    # @staticmethod
    # def _get_deployment_config(flavor_name):
    #
    #     deployment_config = {DEPLOYMENT_CONFIG_KEY_FLAVOR_NAME: flavor_name}
    #     return deployment_config

    @staticmethod
    def _get_sagemaker_config_name(endpoint_name):
        return "{en}-config-{uid}".format(
            en=endpoint_name, uid=get_unique_resource_id()
        )

    def create_sagemaker_model(
        self, model_name, model_s3_path, vpc_config, image_url, execution_role
    ):
        create_model_args = {
            "ModelName": model_name,
            "PrimaryContainer": {
                "ContainerHostname": "mfs-%s" % model_name,
                "Image": image_url,
                # 'ModelDataUrl': model_s3_path
                # 'Environment': SageMakerManager._get_deployment_config(flavor_name=flavor),
            },
            "ExecutionRoleArn": execution_role,
            # "Tags": [{'Key': 'run_id', 'Value': str(run_id)}],
        }
        if vpc_config is not None:
            create_model_args["VpcConfig"] = vpc_config

        model_response = self.client.create_model(**create_model_args)
        return model_response

    def delete_sagemaker_model(self, model_name, s3_client):
        model_info = self.client.describe_model(ModelName=model_name)
        model_arn = model_info["ModelArn"]
        model_data_url = model_info["PrimaryContainer"]["ModelDataUrl"]

        # Parse the model data url to obtain a bucket path. The following
        # procedure is safe due to the well-documented structure of the `ModelDataUrl`
        # (see https://docs.aws.amazon.com/sagemaker/latest/dg/API_ContainerDefinition.html)
        parsed_data_url = urllib.parse.urlparse(model_data_url)
        bucket_data_path = parsed_data_url.path.split("/")
        bucket_name = bucket_data_path[1]
        bucket_key = "/".join(bucket_data_path[2:])

        s3_client.delete_object(Bucket=bucket_name, Key=bucket_key)
        self.client.delete_model(ModelName=model_name)

        return model_arn

    def delete_sagemaker_endpoint_configuration(
        self, endpoint_config_name, sage_client
    ):
        """
        :param sage_client: A boto3 client for SageMaker.
        :return: ARN of the deleted endpoint configuration.
        """
        endpoint_config_info = sage_client.describe_endpoint_config(
            EndpointConfigName=endpoint_config_name
        )
        sage_client.delete_endpoint_config(EndpointConfigName=endpoint_config_name)
        return endpoint_config_info["EndpointConfigArn"]

    def update_sagemaker_endpoint(
        self,
        endpoint_name,
        image_url,
        model_s3_path,
        instance_type,
        instance_count,
        vpc_config,
        mode,
        role,
        s3_client,
    ):

        endpoint_info = self.client.describe_endpoint(EndpointName=endpoint_name)
        endpoint_arn = endpoint_info["EndpointArn"]
        deployed_config_name = endpoint_info["EndpointConfigName"]
        deployed_config_info = self.client.describe_endpoint_config(
            EndpointConfigName=deployed_config_name
        )
        deployed_config_arn = deployed_config_info["EndpointConfigArn"]
        deployed_production_variants = deployed_config_info["ProductionVariants"]

        logging.info("Found active endpoint with arn: %s. Updating...", endpoint_arn)

        new_model_name = SageMakerManager._get_sagemaker_model_name(endpoint_name)
        new_model_response = self.create_sagemaker_model(
            model_name=new_model_name,
            model_s3_path=model_s3_path,
            vpc_config=vpc_config,
            image_url=image_url,
            execution_role=role,
        )
        logging.info("Created new model with arn: %s", new_model_response["ModelArn"])

        if mode == DeployMode.ADD:
            new_model_weight = 0
            production_variants = deployed_production_variants
        elif mode == DeployMode.REPLACE:
            new_model_weight = 1
            production_variants = []

        new_production_variant = {
            "VariantName": new_model_name,
            "ModelName": new_model_name,
            "InitialInstanceCount": instance_count,
            "InstanceType": instance_type,
            "InitialVariantWeight": new_model_weight,
        }
        production_variants.append(new_production_variant)

        # Create the new endpoint configuration and update the endpoint
        # to adopt the new configuration
        new_config_name = SageMakerManager._get_sagemaker_config_name(endpoint_name)
        endpoint_config_response = self.client.create_endpoint_config(
            EndpointConfigName=new_config_name,
            ProductionVariants=production_variants,
            Tags=[{"Key": "app_name", "Value": endpoint_name}],
        )
        logging.info(
            "Created new endpoint configuration with arn: %s",
            endpoint_config_response["EndpointConfigArn"],
        )

        self.client.update_endpoint(
            EndpointName=endpoint_name, EndpointConfigName=new_config_name
        )
        logging.info("Updated endpoint with new configuration!")

        operation_start_time = time.time()

        def status_check_fn():
            if time.time() - operation_start_time < 20:
                # Wait at least 20 seconds before checking the status of the update; this ensures
                # that we don't consider the operation to have failed if small delays occur at
                # initialization time
                return SageMakerOperationStatus.in_progress()

            endpoint_info = self.client.describe_endpoint(EndpointName=endpoint_name)
            endpoint_update_was_rolled_back = (
                endpoint_info["EndpointStatus"] == "InService"
                and endpoint_info["EndpointConfigName"] != new_config_name
            )
            if (
                endpoint_update_was_rolled_back
                or endpoint_info["EndpointStatus"] == "Failed"
            ):
                failure_reason = endpoint_info.get(
                    "FailureReason",
                    (
                        "An unknown SageMaker failure occurred. Please see the SageMaker console logs for"
                        " more information."
                    ),
                )
                return SageMakerOperationStatus.failed(failure_reason)
            elif endpoint_info["EndpointStatus"] == "InService":
                return SageMakerOperationStatus.succeeded(
                    "The SageMaker endpoint was updated successfully."
                )
            else:
                return SageMakerOperationStatus.in_progress(
                    "The update operation is still in progress. Current endpoint status:"
                    ' "{endpoint_status}"'.format(
                        endpoint_status=endpoint_info["EndpointStatus"]
                    )
                )

        def cleanup_fn():
            logging.info("Cleaning up unused resources...")
            if mode == DeployMode.REPLACE:
                for pv in deployed_production_variants:
                    deployed_model_arn = self.delete_sagemaker_model(
                        model_name=pv["ModelName"], s3_client=s3_client
                    )
                    logging.info("Deleted model with arn: %s", deployed_model_arn)

            self.client.delete_endpoint_config(EndpointConfigName=deployed_config_name)
            logging.info(
                "Deleted endpoint configuration with arn: %s", deployed_config_arn
            )

        return SageMakerOperation(
            status_check_fn=status_check_fn, cleanup_fn=cleanup_fn
        )

    def create_sagemaker_endpoint(
        self,
        endpoint_name,
        image_url,
        model_s3_path,
        instance_type,
        vpc_config,
        instance_count,
        role,
    ):
        logging.info("Creating new endpoint with name: %s ...", endpoint_name)

        model_name = self._get_sagemaker_model_name(endpoint_name)
        model_response = self.create_sagemaker_model(
            model_name=model_name,
            model_s3_path=model_s3_path,
            vpc_config=vpc_config,
            image_url=image_url,
            execution_role=role,
        )
        logging.info("Created model with arn: %s", model_response["ModelArn"])

        production_variant = {
            "VariantName": model_name,
            "ModelName": model_name,
            "InitialInstanceCount": instance_count,
            "InstanceType": instance_type,
            "InitialVariantWeight": 1,
        }

        config_name = SageMakerManager._get_sagemaker_config_name(endpoint_name)
        endpoint_config_response = self.client.create_endpoint_config(
            EndpointConfigName=config_name,
            ProductionVariants=[production_variant],
            Tags=[{"Key": "app_name", "Value": endpoint_name}],
        )
        logging.info(
            "Created endpoint configuration with arn: %s",
            endpoint_config_response["EndpointConfigArn"],
        )

        endpoint_response = self.client.create_endpoint(
            EndpointName=endpoint_name, EndpointConfigName=config_name, Tags=[]
        )

        logging.info("Created endpoint with arn: %s", endpoint_response["EndpointArn"])

        def status_check_fn():
            endpoint_info = self.find_endpoint(endpoint_name=endpoint_name)

            if endpoint_info is None:
                return SageMakerOperationStatus.in_progress(
                    "Waiting for endpoint to be created..."
                )

            endpoint_status = endpoint_info["EndpointStatus"]
            if endpoint_status == "Creating":
                return SageMakerOperationStatus.in_progress(
                    'Waiting for endpoint to reach the "InService" state. Current endpoint status:'
                    ' "{endpoint_status}"'.format(endpoint_status=endpoint_status)
                )
            elif endpoint_status == "InService":
                return SageMakerOperationStatus.succeeded(
                    "The SageMaker endpoint was created successfully."
                )
            else:
                failure_reason = endpoint_info.get(
                    "FailureReason",
                    (
                        "An unknown SageMaker failure occurred. Please see the SageMaker console logs for"
                        " more information."
                    ),
                )
                return SageMakerOperationStatus.failed(failure_reason)

        def cleanup_fn():
            pass

        return SageMakerOperation(
            status_check_fn=status_check_fn, cleanup_fn=cleanup_fn
        )


def get_assumed_role_arn():
    """
    :return: ARN of the user's current IAM role.
    """
    sess = boto3.Session()
    sts_client = sess.client("sts")
    identity_info = sts_client.get_caller_identity()
    sts_arn = identity_info["Arn"]
    role_name = sts_arn.split("/")[1]
    iam_client = sess.client("iam")
    role_response = iam_client.get_role(RoleName=role_name)
    return role_response["Role"]["Arn"]


def _get_account_id():
    sess = boto3.Session()
    sts_client = sess.client("sts")
    identity_info = sts_client.get_caller_identity()
    account_id = identity_info["Account"]
    return account_id


def get_default_s3_bucket(region_name):
    # create bucket if it does not exist
    sess = boto3.Session()
    account_id = _get_account_id()
    bucket_name = "{pfx}-{rn}-{aid}".format(
        pfx=DEFAULT_BUCKET_NAME_PREFIX, rn=region_name, aid=account_id
    )
    s3 = sess.client("s3")
    response = s3.list_buckets()
    buckets = [b["Name"] for b in response["Buckets"]]
    if bucket_name not in buckets:
        logging.info("Default bucket `%s` not found. Creating...", bucket_name)
        bucket_creation_kwargs = {
            "ACL": "bucket-owner-full-control",
            "Bucket": bucket_name,
        }
        if region_name != "us-east-1":
            # The location constraint is required during bucket creation for all regions
            # outside of us-east-1. This constraint cannot be specified in us-east-1;
            # specifying it in this region results in a failure, so we will only
            # add it if we are deploying outside of us-east-1.
            # See https://docs.aws.amazon.com/cli/latest/reference/s3api/create-bucket.html#examples
            bucket_creation_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": region_name
            }
        response = s3.create_bucket(**bucket_creation_kwargs)
        logging.info("Bucket creation response: %s", response)
    else:
        logging.info(
            "Default bucket `%s` already exists. Skipping creation.", bucket_name
        )
    return bucket_name


def upload_s3(local_model_path, bucket, prefix, region_name, s3_client):
    sess = boto3.Session(region_name=region_name)
    with TempDir() as tmp:
        model_data_file = tmp.path("model.tar.gz")
        make_tarfile(model_data_file, os.path.dirname(local_model_path))
        with open(model_data_file, "rb") as fobj:
            key = os.path.join(prefix, "model.tar.gz")
            obj = sess.resource("s3").Bucket(bucket).Object(key)
            obj.upload_fileobj(fobj)
            response = s3_client.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={"TagSet": [{"Key": "SageMaker", "Value": "true"}]},
            )
            logging.info("tag response: %s", response)
            return "{}/{}/{}".format(s3_client.meta.endpoint_url, bucket, key)


class SageMakerOperation:
    def __init__(self, status_check_fn, cleanup_fn):
        self.status_check_fn = status_check_fn
        self.cleanup_fn = cleanup_fn
        self.start_time = time.time()
        self.status = SageMakerOperationStatus(
            SageMakerOperationStatus.STATE_IN_PROGRESS, None
        )
        self.cleaned_up = False

    def await_completion(self, timeout_seconds):
        iteration = 0
        begin = time.time()
        while (time.time() - begin) < timeout_seconds:
            status = self.status_check_fn()
            if status.state == SageMakerOperationStatus.STATE_IN_PROGRESS:
                if iteration % 4 == 0:
                    # Log the progress status roughly every 20 seconds
                    logging.info(status.message)

                time.sleep(5)
                iteration += 1
                continue
            else:
                self.status = status
                return status

        duration_seconds = time.time() - begin
        return SageMakerOperationStatus.timed_out(duration_seconds)

    def clean_up(self):
        if self.status.state != SageMakerOperationStatus.STATE_SUCCEEDED:
            raise ValueError(
                "Cannot clean up an operation that has not succeeded! Current operation state:"
                " {operation_state}".format(operation_state=self.status.state)
            )

        if not self.cleaned_up:
            self.cleaned_up = True
        else:
            raise ValueError(
                "`clean_up()` has already been executed for this operation!"
            )

        self.cleanup_fn()


class SageMakerOperationStatus:
    STATE_SUCCEEDED = "succeeded"
    STATE_FAILED = "failed"
    STATE_IN_PROGRESS = "in progress"
    STATE_TIMED_OUT = "timed_out"

    def __init__(self, state, message):
        self.state = state
        self.message = message

    @classmethod
    def in_progress(cls, message=None):
        if message is None:
            message = "The operation is still in progress."
        return cls(SageMakerOperationStatus.STATE_IN_PROGRESS, message)

    @classmethod
    def timed_out(cls, duration_seconds):
        return cls(
            SageMakerOperationStatus.STATE_TIMED_OUT,
            "Timed out after waiting {duration_seconds} seconds for the operation to"
            " complete. This operation may still be in progress. Please check the AWS"
            " console for more information.".format(duration_seconds=duration_seconds),
        )

    @classmethod
    def failed(cls, message):
        return cls(SageMakerOperationStatus.STATE_FAILED, message)

    @classmethod
    def succeeded(cls, message):
        return cls(SageMakerOperationStatus.STATE_SUCCEEDED, message)
