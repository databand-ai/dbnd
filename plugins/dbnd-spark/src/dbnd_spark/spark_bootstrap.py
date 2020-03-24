from dbnd._core.utils.seven import import_errors


_DBND_REGISTER_SPARK_TYPES = None


def dbnd_spark_bootstrap():
    global _DBND_REGISTER_SPARK_TYPES
    if _DBND_REGISTER_SPARK_TYPES:
        return
    # don't run it twice or in recursion
    _DBND_REGISTER_SPARK_TYPES = True

    try:
        import pyspark
    except import_errors as ex:
        # pyspark is not installed, probably user will not use pyspark types
        return
    # we register spark types only if we have spark installed
    try:
        from dbnd_spark.targets import dbnd_register_spark_types

        dbnd_register_spark_types()
    except Exception:
        pass
    return
