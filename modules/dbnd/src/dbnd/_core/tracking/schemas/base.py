from dbnd._vendor.marshmallow import Schema


class ApiObjectSchema(Schema):
    class Meta:
        strict = True


class _ApiCallSchema(Schema):
    class Meta:
        strict = True
