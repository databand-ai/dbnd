import dbnd

from dbnd import register_config_cls
from targets.fs import register_file_system


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_hdfs.fs.hdfs_hdfscli import HdfsCli
    from dbnd_hdfs.fs.hdfs_pyox import HdfsPyox

    register_config_cls(HdfsCli)
    register_config_cls(HdfsPyox)

    from dbnd_hdfs.fs.hdfs import create_hdfs_client

    register_file_system("hdfs", create_hdfs_client)
