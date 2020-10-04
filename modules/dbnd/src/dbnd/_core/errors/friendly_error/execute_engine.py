from dbnd._core.errors import DatabandConfigError


def parallel_or_remote_sqlite(executor):  # type: (str) -> DatabandConfigError
    return DatabandConfigError(
        "'%s' executor is not supported when using an sqlite database" % executor,
        help_msg="Please switch to a different database for all parallel and "
        "remote executions (we recommend PostgreSQL)",
    )


def parallel_with_inprocess(executor_type):
    return DatabandConfigError(
        "We don not support parallel execution "
        "in {executor_type} executor type".format(executor_type=executor_type)
    )
