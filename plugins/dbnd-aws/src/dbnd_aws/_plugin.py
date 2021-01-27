import dbnd

from dbnd import register_config_cls
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from targets.fs import FileSystems, register_file_system


@dbnd.hookimpl
def dbnd_setup_plugin():
    # register configs
    from dbnd_aws.env import AwsEnvConfig

    register_config_cls(AwsEnvConfig)

    if is_plugin_enabled("dbnd-spark"):
        from dbnd_aws.emr.emr_config import EmrConfig

        register_config_cls(EmrConfig)

    if is_plugin_enabled("dbnd-docker"):
        from dbnd_aws.batch.aws_batch_ctrl import AwsBatchConfig

        register_config_cls(AwsBatchConfig)

    from dbnd_aws.fs import build_s3_fs_client

    register_file_system(FileSystems.s3, build_s3_fs_client)
