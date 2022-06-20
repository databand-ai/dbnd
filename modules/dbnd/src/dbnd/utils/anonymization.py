import re

from typing import List


DEFAULT_MAKING_VALUE = "***"


class DataAnonymizer:
    def __init__(self, pattern: str, masking_expression: str = DEFAULT_MAKING_VALUE):
        self.pattern = pattern
        self.masking_expression = masking_expression

    def anonymize(self, raw_data):
        if not raw_data:
            return raw_data

        anonymize_data = raw_data
        finds = re.findall(self.pattern, anonymize_data, flags=re.IGNORECASE)
        for find in finds:
            if isinstance(find, tuple):
                for item in find:
                    anonymize_data = anonymize_data.replace(
                        item, self.masking_expression
                    )
            else:
                anonymize_data = anonymize_data.replace(find, self.masking_expression)

        return anonymize_data


class CompositeDataAnonymizer:
    def __init__(self, data_anonymizers: List[DataAnonymizer]):
        self.data_anonymizers = data_anonymizers

    @classmethod
    def from_string_patterns(cls, patterns: List[str]):
        anonymizers = [DataAnonymizer(pattern) for pattern in patterns]
        return cls(data_anonymizers=anonymizers)

    def anonymize(self, data: str):
        res = data
        for anonymizer in self.data_anonymizers:
            res = anonymizer.anonymize(res)

        return res


aws_credentials_regexp = [
    r"::(?P<IAM_TO_KEEP>\d{12}):role",  # IAM Role
    r"aws(?:_access)?_key_id[\s=']*(AKI[a-zA-Z0-9]+)[';]",  # Redshift access key
    r"aws_secret(?:_access)?_key[\s=']*([a-zA-Z0-9/]+)[';]",  # Redshift secret key
]


aws_secrets_anonymizer = CompositeDataAnonymizer.from_string_patterns(
    aws_credentials_regexp
)

query_data_annonymizer = CompositeDataAnonymizer([aws_secrets_anonymizer])
