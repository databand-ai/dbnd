# © Copyright Databand.ai, an IBM Company 2022

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

    main_jar = parameter(description="Set the path to the main application jar.").none[
        str
    ]

    driver_class_path = parameter.c(
        description="Determine additional, driver-specific, classpath settings."
    ).none[str]
    jars = parameter.c(
        description="Submit additional jars to upload and place them in the executor classpath."
    )[List[str]]

    py_files = parameter.c(
        description="Set any additional python files used by the job. This can be .zip, .egg or .py."
    )[List[str]]
    files = parameter.c(
        description="Upload additional files to the executor running the job, "
        "separated by a comma. "
        "Files will be placed in the working directory "
        "of each executor. For example, serialized objects."
    )[List[str]]

    packages = parameter(
        description="Set a comma-separated list of maven coordinates of jars to include on the driver and executor "
        "classpaths."
    ).none[str]
    exclude_packages = parameter(
        description="Comma-separated list of maven coordinates of jars to exclude while resolving the dependencies "
        "provided in `packages`."
    ).none[str]
    repositories = parameter(
        description="Comma-separated list of additional remote repositories to search for the maven coordinates given "
        "with `packages`."
    ).none[str]

    conf = parameter.c(description="Set arbitrary Spark configuration properties.")[
        Dict[str, str]
    ]

    num_executors = parameter.c(
        description="Determine the number of executors to launch."
    )[int]
    total_executor_cores = parameter(
        description="Set the number of total cores for all executors. This is only applicable for standalone and Mesos."
    ).none[int]
    executor_cores = parameter(
        description="Set the number of cores per executor. This is only applicable for Standalone and YARN."
    ).none[int]
    status_poll_interval = parameter(
        default=5,
        description="Set the number of seconds to wait between polls of driver status in the cluster.",
    )[int]

    executor_memory = parameter(
        description="Set the amount of memory per executor, e.g. 1000M, 2G. The default value is 1G."
    ).none[str]
    driver_memory = parameter(
        description="Set the amount of memory allocated to the driver, e.g. 1000M, 2G. The default value is 1G."
    ).none[str]

    driver_cores = parameter(
        description="Set the number of cores in the driver. This is only applicable for Livy."
    ).none[int]

    queue = parameter(description="Set the YARN queue to submit to.").none[str]
    proxy_user = parameter(
        description="Set the user to impersonate when submitting the application. "
        "This argument does not work with `--principal` or `--keytab`."
    ).none[str]
    archives = parameter.c(
        description="Set a comma separated list of archives to be extracted into the "
        "working directory of each executor."
    )[List[str]]

    keytab = parameter(
        description="Set the full path to the file that contains the keytab."
    ).none[str]
    principal = parameter.c(
        description="Set the name of the Kerberos principal used for keytab."
    )[str]

    env_vars = parameter.c(
        description="Set the environment variables for spark-submit. It supports yarn and k8s mode too."
    )[Dict[str, str]]
    verbose = parameter.value(
        False,
        description="Determine whether to pass the verbose flag to the spark-submit process for debugging",
    )

    deploy_mode = parameter.c(
        description="Set the driver mode of the spark submission."
    )[str]

    # databand specific
    submit_args = parameter.none.help(
        "Set spark arguments as a string, e.g. `--num-executors 10`"
    )[str]

    disable_sync = parameter.value(
        False, description="Disable databand auto-sync mode for Spark files."
    )

    disable_tracking_api = parameter.value(
        False,
        description="Disable saving metrics and DataFrames (so log_metric and log_dataframe will just print to the spark log)."
        " Set this to true if you can't configure connectivity from the Spark cluster to the databand server.",
    )

    use_current_spark_session = parameter.value(
        False,
        description="If Spark Session exists, do not send to remote cluster/spark-submit, but use existing.",
    )

    include_user_project = parameter.c(
        default=False,
        description="Enable building fat_wheel from configured package and third-party requirements"
        " (configured in bdist_zip section) and upload it to Spark",
    )[bool]

    fix_pyspark_imports = parameter(
        description="Determine whether databand should reverse import resolution order when running within spark.",
        default=False,
    )[bool]

    # This config value is used to minimize spark startup time. Due to spark's filesystem layout, pluggy
    # loading entrypoints takes <30 seconds, this config disables that and speeds up run duration
    disable_pluggy_entrypoint_loading = parameter(
        description="When set to true, databand will not load any plugins within spark execution, other than the "
        "plugins loaded during spark submission.",
        default=False,
    )[bool]


class SparkEngineConfig(Config):
    root = parameter(default=None, description="Data outputs location override")[
        DirTarget
    ]

    disable_task_band = parameter(
        default=False, description="Disable task_band file creation"
    )[bool]
