from typing import Dict, List

from dbnd import parameter
from dbnd._core.task.config import Config
from targets import DirTarget


class SparkConfig(Config):
    """Apache Spark (-s [TASK].spark.[PARAM]=[VAL] for specific tasks)"""

    # we don't want spark class to inherit from this one, as it should has Config behaviour
    _conf__task_family = "spark"
    _conf__help_description = (
        "spark jobs configuration, "
        "use --conf TASK_NAME.spark.parameter=value in order to override specific task value"
    )

    main_jar = parameter(description="Main application jar").none[str]

    driver_class_path = parameter.c(
        description="Additional, driver-specific, classpath settings"
    ).none[str]
    jars = parameter.c(
        description="Submit additional jars to upload and place them in executor classpath."
    )[List[str]]

    py_files = parameter.c(
        description="Additional python files used by the job, can be .zip, .egg or .py."
    )[List[str]]
    files = parameter.c(
        description="Upload additional files to the executor running the job,"
        " separated by a comma. "
        "Files will be placed in the working directory "
        "of each executor.For example, serialized objects"
    )[List[str]]

    packages = parameter(
        description="Comma-separated list of maven coordinates of jars to \
                                                     include on the driver and executor classpaths."
    ).none[str]
    exclude_packages = parameter(
        description="Comma-separated list of maven coordinates of \
                                  jars to exclude while resolving the dependencies provided in 'packages'"
    ).none[str]
    repositories = parameter(
        description="Comma-separated list of additional remote repositories\
                                                      to search for the maven coordinates given with 'packages'"
    ).none[str]

    conf = parameter.c(description="Arbitrary Spark configuration properties")[
        Dict[str, str]
    ]

    num_executors = parameter.c(description="Number of executors to launch")[int]
    total_executor_cores = parameter(
        description="(Standalone & Mesos only) Total cores for all executors"
    ).none[int]
    executor_cores = parameter(
        description="(Standalone & YARN only) Number of cores per executor"
    ).none[int]
    executor_memory = parameter(
        description="Memory per executor (e.g. 1000M, 2G) (Default: 1G)"
    ).none[str]
    driver_memory = parameter(
        description="Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)"
    ).none[str]

    driver_cores = parameter(description="(Liby only) Number of cores in driver").none[
        int
    ]

    queue = parameter(
        description="The YARN queue to submit to (Default: 'default')."
    ).none[str]
    proxy_user = parameter(
        description="User to impersonate when submitting the application. "
        "This argument does not work with --principal / --keytab."
    ).none[str]
    archives = parameter.c(
        description="Comma separated list of archives to be extracted into the"
        "working directory of each executor."
    )[List[str]]

    keytab = parameter(
        description="Full path to the file that contains the keytab"
    ).none[str]
    principal = parameter.c(
        description="The name of the kerberos principal used for keytab"
    )[str]

    env_vars = parameter.c(
        description="Environment variables for spark-submit. It supports yarn and k8s mode too."
    )[Dict[str, str]]
    verbose = parameter.value(
        False,
        description="Whether to pass the verbose flag "
        "to spark-submit process for debugging",
    )

    deploy_mode = parameter.c(description="Driver mode of the spark submission")[str]

    # databand specific
    submit_args = parameter.none.help(
        "spark arguments as a string, like: --num-executors 10"
    )[str]

    disable_sync = parameter.value(
        False, description="Disable databand auto sync mode for spark files"
    )

    disable_tracking_api = parameter.value(
        False,
        description="Disable saving of metrics and dataframes (so log_metric and log_dataframe will just print to the spark log)."
        + " Set this to true if you can't configure connectivity from the spark cluster to the databand server.",
    )

    use_current_spark_session = parameter.value(
        False,
        description="If Spark Session exists, "
        "do not send to remote cluster/spark-submit, but use existing",
    )

    include_user_project = parameter.c(
        default=False,
        description="Build fat_wheel from configured package and third-party requirements"
        + " (configured in bdist_zip section) and upload it to Spark",
    )[bool]


class SparkEngineConfig(Config):
    root = parameter(default=None, description="Data outputs location override")[
        DirTarget
    ]

    disable_task_band = parameter(
        default=False, description="Disable task_band file creation"
    )[bool]


class SparkMarshallingConfig(Config):
    _conf__task_family = "spark_marshalling"
    default_infer_schema_value = parameter(
        default=False,
        description="Should the DataFrame to CSV "
        "marshaller infer schema by default. Can be overridden via `parameter.save_options` or "
        "`parameter.load_options` on the relevant parameter",
    )[bool]
    default_header_value = parameter(
        default=False,
        description="Should the DataFrame to CSV "
        "marshaller set header by default. Can be overridden via `parameter.save_options` or "
        "`parameter.load_options` on the relevant parameter",
    )[bool]
