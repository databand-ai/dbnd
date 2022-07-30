# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd.utils.anonymization import query_data_annonymizer


@pytest.mark.parametrize(
    "query, masked_query",
    [
        (
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-redshift/deniro.csv'
        iam_role 'arn:aws:iam::111111111111:role/myRedshiftRole' csv;
        """,
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-redshift/deniro.csv'
        iam_role 'arn:aws:iam::***:role/myRedshiftRole' csv;
        """,
        ),
        (
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-redshift/deniro.csv'
        CREDENTIALS
        'aws_access_key_id=AKIAIOSFODNN7EXAMPLE;aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        """,
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-redshift/deniro.csv'
        CREDENTIALS
        'aws_access_key_id=***;aws_secret_access_key=***'
        """,
        ),
        (
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-redshift/deniro.csv'
        aws_access_key_id 'AKIAIOSFODNN7EXAMPLE' aws_secret_access_key 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        """,
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-redshift/deniro.csv'
        aws_access_key_id '***' aws_secret_access_key '***'
        """,
        ),
        (
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-redshift/deniro.csv'
        """,
            """
        COPY "DENIRO_MOVIES_RATINGS" (year, score, title) from 's3://dbnd-redshift/deniro.csv'
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
    result = query_data_annonymizer.anonymize(query)
    assert result == masked_query
