import logging

from typing import List

import pandas as pd

from dbnd import dbnd_handle_errors, pipeline, task


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


if __name__ == "__main__":
    with dbnd_handle_errors():
        experiment_pipeline.dbnd_run(task_version="now")
