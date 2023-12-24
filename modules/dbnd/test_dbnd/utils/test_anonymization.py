# © Copyright Databand.ai, an IBM Company 2022
import json

from abc import ABC, abstractmethod

import pytest

from dbnd._core.utils.data_anonymizers import mask_sensitive_data


SECRETS_LIST = [
    "aws_key_id",
    "aws_access_key_id",
    "aws_secret_key",
    "aws_secret_access_key",
    "my_secret",
    "secret_of_mine",
    "redshift_password",
    "another_password_of_mine",
    "another__secret_of_mine",
    "identity_token_api_path",
    "private_key_passphrase",
    "IAM role: arn:aws:iam:",
]

NON_SECRETS_LIST = [
    "aws_user",
    "aws_port",
    "aaaa",
    "example",
    "private_car",
    "second_time",
]

PARAM_VALUE = "abc" * 10

EXPECTED_MASKED_OUTPUT = "***"


def to_python_dict(param_name: str, param_value: str = PARAM_VALUE):
    return f"'{param_name}':'{param_value}'"


class MaskSensitiveDataTest(ABC):
    @abstractmethod
    def format_to_string(self, data_to_mask: dict) -> str:
        raise NotImplementedError()

    def should_mask_values_for_sensitive_param_names(self, param_name: str):
        # Arrange
        raw_data = {param_name: PARAM_VALUE}
        raw_string = self.format_to_string(raw_data)

        # Act
        masked_string = mask_sensitive_data(raw_string)

        # Assert
        expected_output = {param_name: EXPECTED_MASKED_OUTPUT}
        assert masked_string == self.format_to_string(expected_output)

    def should_not_mask_values_non_sensitive_param_names(self, param_name: str):
        # Arrange
        raw_data = {param_name: PARAM_VALUE}
        raw_string = self.format_to_string(raw_data)

        # Act
        masked_string = mask_sensitive_data(raw_string)

        # Assert
        expected_output = raw_data
        assert masked_string == self.format_to_string(expected_output)


class TestMaskingSensitiveData(MaskSensitiveDataTest):
    def format_to_string(self, data_to_mask: dict) -> str:
        return json.dumps(data_to_mask)

    @pytest.mark.parametrize("param_name", [(param) for param in SECRETS_LIST])
    def test_should_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_mask_values_for_sensitive_param_names(param_name)

    @pytest.mark.parametrize("param_name", [(param) for param in NON_SECRETS_LIST])
    def test_should_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_not_mask_values_non_sensitive_param_names(param_name)


class TestMaskingSensitiveDataEnvVariableFormat(MaskSensitiveDataTest):
    def format_to_string(self, data_to_mask: dict) -> str:
        res = ""
        for k, v in data_to_mask.items():
            res += f"{k.upper()}={v.upper()}"

        return res

    @pytest.mark.parametrize("param_name", [(param) for param in SECRETS_LIST])
    def test_should_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_mask_values_for_sensitive_param_names(param_name)

    @pytest.mark.parametrize("param_name", [(param) for param in NON_SECRETS_LIST])
    def test_should_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_not_mask_values_non_sensitive_param_names(param_name)


class TestMaskingSensitiveDataYaml1Format(MaskSensitiveDataTest):
    def format_to_string(self, data_to_mask: dict) -> str:
        res = ""
        for param_name, param_value in data_to_mask.items():
            res += f"{param_name}='{param_value}'"

        return res

    @pytest.mark.parametrize("param_name", [(param) for param in SECRETS_LIST])
    def test_should_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_mask_values_for_sensitive_param_names(param_name)

    @pytest.mark.parametrize("param_name", [(param) for param in NON_SECRETS_LIST])
    def test_should_not_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_not_mask_values_non_sensitive_param_names(param_name)


class TestMaskingSensitiveDataYaml2Format(MaskSensitiveDataTest):
    def format_to_string(self, data_to_mask: dict) -> str:
        res = ""
        for param_name, param_value in data_to_mask.items():
            res += f"{param_name}: '{param_value}'"

        return res

    @pytest.mark.parametrize("param_name", [(param) for param in SECRETS_LIST])
    def test_should_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_mask_values_for_sensitive_param_names(param_name)

    @pytest.mark.parametrize("param_name", [(param) for param in NON_SECRETS_LIST])
    def test_should_not_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_not_mask_values_non_sensitive_param_names(param_name)


class TestMaskingSensitiveDataPythonDictFormat(MaskSensitiveDataTest):
    def format_to_string(self, data_to_mask: dict) -> str:
        return str(data_to_mask)

    @pytest.mark.parametrize("param_name", [(param) for param in SECRETS_LIST])
    def test_should_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_mask_values_for_sensitive_param_names(param_name)

    @pytest.mark.parametrize("param_name", [(param) for param in NON_SECRETS_LIST])
    def test_should_mask_values_for_sensitive_param_names(self, param_name: str):
        self.should_not_mask_values_non_sensitive_param_names(param_name)


def test_multiple_calls():
    secret_value = json.dumps({"password": "12344567"})  # pragma: allowlist secret
    for _ in range(5):
        secret_value = mask_sensitive_data(secret_value)

    assert secret_value == '{"password": "***"}'


@pytest.mark.parametrize(
    "query, masked_query",
    [
        (
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-dev-redshift/deniro.csv'
        iam_role 'arn:aws:iam::111111111111:role/redshift-s3' csv;
        """,
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-dev-redshift/deniro.csv'
        iam_role 'arn:aws:iam::***:role/redshift-s3' csv;
        """,
        ),
        (
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-dev-redshift/deniro.csv'
        CREDENTIALS
        'aws_access_key_id=AKIAIOSFODNN7EXAMPLE;aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        """,
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-dev-redshift/deniro.csv'
        CREDENTIALS
        'aws_access_key_id=***;aws_secret_access_key=***'
        """,
        ),
        (
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-dev-redshift/deniro.csv'
        aws_access_key_id 'AKIAIOSFODNN7EXAMPLE' aws_secret_access_key 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        """,
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-dev-redshift/deniro.csv'
        aws_access_key_id '***' aws_secret_access_key '***'
        """,
        ),
        (
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-dev-redshift/deniro.csv'
        """,
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-dev-redshift/deniro.csv'
        """,
        ),
        (
            """
        copy into emp
        from s3://mybucket/mypath/
            credentials = (aws_key_id = 'AKIAIOSFODNN7EXAMPLE' aws_secret_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
            file_format = (
              type = csv
              field_delimiter = '\001'
              null_if = ('\\N')
            );
        """,
            """
        copy into emp
        from s3://mybucket/mypath/
            credentials = (aws_key_id = '***' aws_secret_key = '***')
            file_format = (
              type = csv
              field_delimiter = '\001'
              null_if = ('\\N')
            );
        """,
        ),
        (
            """
        copy into emp
        from s3://mybucket/mypath/
            credentials = (aws_role = 'arn:aws:iam::123456789123:role/SnowflakeStageRole')
            file_format = (
              type = csv
              field_delimiter = '\001'
              null_if = ('\\N')
            );
          """,
            """
        copy into emp
        from s3://mybucket/mypath/
            credentials = (aws_role = 'arn:aws:iam::***:role/SnowflakeStageRole')
            file_format = (
              type = csv
              field_delimiter = '\001'
              null_if = ('\\N')
            );
          """,
        ),
        (None, None),
        ("aws_access_key_id=AKIAIOSFODNN7EXAMPLE;", "aws_access_key_id=***;"),
    ],
)
def test_query_data_anonymyzation(query, masked_query):
    """
    In this test function the 'secrets' are example values in order to match regex
    taken from https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html
    """
    result = mask_sensitive_data(query)
    assert result == masked_query


@pytest.mark.parametrize(
    "spark_submit, masked_spark_submit",
    [
        (
            "Cannot execute: /opt/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit --master local "
            "--conf spark.sql.shuffle.partitions=1"
            "--conf spark.env.DBND__CORE__DATABAND_URL=https://databand.ai "
            "--conf spark.env.DBND__CORE__DATABAND_ACCESS_TOKEN=eyJ0eXAiOiJKV1QGciOiJIUzI1NiJ9.eyJpYXQm5iZiI6MTY1Mjk3NTk1MCwianRpIjoiZGIzM2JjNTAtMGZlMS00MDExLWFiZmYtODJiNThjZDk2OTgyIiwiZXhwIjoxNzE2MDQ3OTUwLCJpZGVudGl0eSI6ImRhdGFiYW5kIiwiZnJlc2giOmZhbHNlLCJ0eXBlIjoiYWNjZXNzIiwidXNlcl9jbGFpbXMiOnsiZW52IjoiZGF0YWJhbmQtaW50ZXJuYWwtcmVsZWFzZSJ9fQ.cgoXFPGKoIU6yUd7IwW8rs "
            "--conf spark.env.DBND__TRACKING=True"
            "--conf spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener",
            "Cannot execute: /opt/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit --master local "
            "--conf spark.sql.shuffle.partitions=1"
            "--conf spark.env.DBND__CORE__DATABAND_URL=https://databand.ai "
            "--conf spark.env.DBND__CORE__DATABAND_ACCESS_TOKEN=*** "
            "--conf spark.env.DBND__TRACKING=True"
            "--conf spark.sql.queryExecutionListeners=ai.databand.spark.DbndSparkQueryExecutionListener",
        )
    ],
)
def test_dbnd_token_anonymization(spark_submit, masked_spark_submit):
    result = mask_sensitive_data(spark_submit)
    assert result == masked_spark_submit
