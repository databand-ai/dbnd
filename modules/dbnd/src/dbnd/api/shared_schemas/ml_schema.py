import attr

from dbnd._vendor.marshmallow import fields, post_load
from dbnd.api.api_utils import _ApiCallSchema


class MLSaveModelSchema(_ApiCallSchema):
    model = fields.Dict()  # pickel
    job_name = fields.String()
    is_enabled = fields.Boolean(allow_none=True)
    data_set = fields.String(allow_none=True)  # df?

    @post_load
    def make_object(self, data, **kwargs):
        return MLSaveModel(**data)


ml_save_model_schema = MLSaveModelSchema()


@attr.s
class MLSaveModel(object):
    model = attr.ib()  # type: Dict
    job_name = attr.ib()  # type: str
    is_enabled = attr.ib(default=False)  # type: Optional[bool]
    data_set = attr.ib(default=None)  # type: Optional[str]
