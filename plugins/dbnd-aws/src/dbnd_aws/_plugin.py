import dbnd

from dbnd import register_config_cls


@dbnd.hookimpl
def dbnd_setup_plugin():
    # register configs
    from dbnd_aws.emr.emr_config import EmrConfig
    from dbnd_aws.batch.aws_batch_ctrl import AwsBatchConfig
    from dbnd_aws.env import AwsEnvConfig

    register_config_cls(EmrConfig)
    register_config_cls(AwsBatchConfig)
    register_config_cls(AwsEnvConfig)
