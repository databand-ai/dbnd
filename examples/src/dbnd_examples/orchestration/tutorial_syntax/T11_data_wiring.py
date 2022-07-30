# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List

import pandas as pd

from dbnd import config, dbnd_handle_errors, pipeline, task


logger = logging.getLogger(__name__)


@task
def operation_A(x_input="x"):
    logger.info("Running  %s -> operation_x", x_input)
    if x_input == "ha":
        raise Exception()
    return "{} -> operation_x".format(x_input)


@pipeline
def pipe_A_operations(pipe_argument="pipe"):
    z = operation_A(pipe_argument)
    x = operation_A(z)
    y = operation_A(x, task_name="zzz")

    # this operation is not wired to any outputs or return values
    # but we need it to run, so it will be "related" to pipe_operations automatically
    operation_A("standalone")

    # you will see y outputs as pipe_argument output in UI
    return y


if __name__ == "__main__":
    with config({"zzz": {"x_input": "ha2"}}):
        operations_task = pipe_A_operations.task(task_version="now")
        operations_task.dbnd_run()


@task
def get_input_data() -> pd.DataFrame:
    return pd.DataFrame([["A"], ["B"], ["C"]], columns=["Names"])


@task
def run_experiment(df: pd.DataFrame, exp_name, ratio) -> pd.DataFrame:
    return pd.DataFrame(
        [[exp_name, ratio, len(df)]], columns=["Names", "Ratio", "Length"]
    )


@task
def build_experiment_report(exp_results: List[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(exp_results)


@pipeline
def experiment_pipeline():
    input_data = get_input_data()
    experiments = [
        run_experiment(input_data, "exp_%s" % ratio, ratio) for ratio in range(100)
    ]
    report = build_experiment_report(experiments)
    return report


@task
def gen_token(t):
    return t


@task
def concat_tokens(tokens_list: List[str]):
    logging.warning("TOKENS: %s", tokens_list)


@pipeline
def experiment_concat():
    # if type is specified, we can mix between strings and task outputs
    l = ["aa", gen_token("bbb")]
    return concat_tokens(l)

    # this operation is not wired to any outputs or return values
    # but we need it to run, so it will be "related" to pipe_operations automatically
    operation_z("standalone")


if __name__ == "__main__":
    with dbnd_handle_errors():
        experiment_pipeline.dbnd_run(task_version="now")
