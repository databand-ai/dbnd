import dbnd

from dbnd import register_config_cls


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_hdfs.fs.hdfs_hdfscli import HdfsCli
    from dbnd_hdfs.fs.hdfs_pyox import HdfsPyox

    register_config_cls(HdfsCli)
    register_config_cls(HdfsPyox)
