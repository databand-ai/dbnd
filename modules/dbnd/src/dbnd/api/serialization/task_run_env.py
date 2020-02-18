from dbnd._core.tracking.tracking_info_objects import TaskRunEnvInfo
from dbnd._vendor.marshmallow import fields, post_load
from dbnd.api.api_utils import ApiObjectSchema


class TaskRunEnvInfoSchema(ApiObjectSchema):
    uid = fields.UUID()
    cmd_line = fields.String()

    user = fields.String()
    machine = fields.String()
    databand_version = fields.String()

    user_code_version = fields.String()
    user_code_committed = fields.Boolean()
    project_root = fields.String()

    user_data = fields.String()

    heartbeat = fields.DateTime()

    @post_load
    def make_object(self, data, **kwargs):
        return TaskRunEnvInfo(**data)
