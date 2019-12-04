import boto3

import requests


def get_current_region():
    """
    works only from ec2 machine
    :return:
    """
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = r.json()
    return response_json.get("region")


def get_boto_emr_client(region_name=None):
    return boto3.client("emr", region_name=region_name)
