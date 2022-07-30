# © Copyright Databand.ai, an IBM Company 2022

import time

from dbnd import log_metric, pipeline, task
from dbnd._core.current import current_task_run


@task
def error_task():
    raise Exception("An explicit error for UI functionality validation.")


@task
def child_has_error():
    try:
        error_task()
    except Exception:
        pass


@task
def external_links_task():
    tr = current_task_run()
    tr.set_external_resource_urls({"test_url": "https://databand.ai/"})
    return None


@task
def figure_output_task():
    # type: ()-> Figure

    import matplotlib
    import matplotlib.pyplot as plt
    import numpy as np

    matplotlib.use("Agg")

    fig = plt.figure()
    ax1 = fig.add_subplot(2, 2, 1)
    ax1.hist(np.random.randn(100), bins=20, alpha=0.3)
    ax2 = fig.add_subplot(2, 2, 2)
    ax2.scatter(np.arange(30), np.arange(30) + 3 * np.random.randn(30))
    fig.add_subplot(2, 2, 3)
    return fig


@task
def log_metrics(param="param"):
    log_metric("param", param)
    log_metric("string_metrics", "Pi")
    log_metric("float_metric", 3.14159265359)
    log_metric("int_metric", 0)


@task
def a_10_sec_task():
    print("Waiting for 10 seconds...")
    time.sleep(10)


@task
def yet_another_task():
    return 10


@pipeline
def yet_another_pipeline():
    return yet_another_task()


@pipeline
def all_ui_function(input="some_input"):
    # type: ()-> Figure

    """
    Running this pipeline should create synthetic information
    that allows us to present relevant data for all UI functionality.

    Each new UI feature should be supported within this class with a matching task supplying data to it.
    """

    child_has_error()
    external_links_task()
    fig = figure_output_task()
    log_metrics(input)
    yet_another_pipeline()
    a_10_sec_task()

    return fig
