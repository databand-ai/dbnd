# © Copyright Databand.ai, an IBM Company 2022

from dbnd._vendor.marshmallow import Schema


class ApiStrictSchema(Schema):
    class Meta:
        strict = True
