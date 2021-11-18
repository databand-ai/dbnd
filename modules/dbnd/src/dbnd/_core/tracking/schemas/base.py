from dbnd._vendor.marshmallow import Schema


class ApiStrictSchema(Schema):
    class Meta:
        strict = True
