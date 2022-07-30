# © Copyright Databand.ai, an IBM Company 2022

import itertools as it
import json
import os
import time

import pandas as pd

from .process import process_customer_data


# Pattern for `Spark History` URL.
url_pattern = (
    "https://p-1rs17wxdw2xgf.emrappui-prod.us-east-2.amazonaws.com/shs/history/{}/jobs/"
)

exp_input_keys = [
    "input_file",
    "output_file",
    "to_pandas",
    "with_histograms",
    "sampling_type",
    "sampling_fraction",
    "columns_number_multiplicator",
]

columns = [
    "app_id",
    "url",
    "input_file",
    "summary_time",
    "distinct_time",
    "histograms_time",
    "total_time",
    "columns_number",
    "rows_number",
    "to_pandas",
    "with_histograms",
    "sampling_type",
    "sampling_fraction",
    "columns_number_multiplicator",
]


def _update_report(app_id, inputs_shape, input_keys, input_values):
    # Copy spark log files from hdfs file system to local file system for python access.
    os.system(f"hdfs dfs -copyToLocal hdfs:///var/log/spark/apps/{app_id} ./logs/")

    logs = [json.loads(line) for line in open(f"./logs/{app_id}").readlines()]

    jobs_start_stats = {
        log["Job ID"]: log for log in logs if log["Event"] == "SparkListenerJobStart"
    }
    jobs_end_stats = {
        log["Job ID"]: log for log in logs if log["Event"] == "SparkListenerJobEnd"
    }

    job_stats = [
        (jobs_start_stats.get(job_id, None), jobs_end_stats.get(job_id, None))
        for job_id in jobs_end_stats.keys()
    ]

    histograms_time = 0.0
    distinct_time = 0.0
    summary_time = 0.0
    total_time = 0.0

    histograms_computation_line = (
        "collect at /home/hadoop/.local/lib/python3.6/site-packages/dbnd_spark/"
        "spark_targets/spark_values.py:314"
    )
    distinct_computation_line = (
        "collect at /home/hadoop/.local/lib/python3.6/site-packages/dbnd_spark/"
        "spark_targets/spark_values.py:161"
    )
    summary_computation_line = "summary at NativeMethodAccessorImpl.java:0"

    for job_start, job_end in job_stats:
        if job_start is not None and job_end is not None:
            callsite = job_start["Stage Infos"][0]["RDD Info"][0]["Callsite"]
            duration = job_end["Completion Time"] - job_start["Submission Time"]

            if callsite.startswith(histograms_computation_line):
                histograms_time += duration
            if callsite.startswith(summary_computation_line):
                summary_time += duration
            if callsite.startswith(distinct_computation_line):
                distinct_time += duration

            total_time += duration

    report_item = {
        "app_id": app_id,
        "url": url_pattern.format(app_id),
        "total_time": total_time,
        "histograms_time": histograms_time,
        "summary_time": summary_time,
        "distinct_time": distinct_time,
        "columns_number": inputs_shape[0],
        "rows_number": inputs_shape[1],
    }
    report_item.update(dict(zip(input_keys, input_values)))

    return report_item


def generate_reports(exp_name, exp_params):
    max_retries = 3
    report_filename = f"reports/{exp_name}_report.tsv"

    exp_input_combinations = [exp_params[key] for key in exp_input_keys]

    for i, exp_input_values in enumerate(it.product(*exp_input_combinations)):
        app_id, inputs_shape = process_customer_data(
            f"{exp_name}_{i}", *exp_input_values
        )

        for j in range(max_retries):
            try:
                report = _update_report(
                    app_id, inputs_shape, exp_input_keys, exp_input_values
                )

                try:
                    reports = pd.read_csv(report_filename, sep="\t").append(
                        report, ignore_index=True
                    )
                except FileNotFoundError:
                    reports = pd.DataFrame([report])

                reports[columns].to_csv(report_filename, sep="\t", index=False)
                print(
                    f"Successfully write report for {exp_name}_{i} to the file {report_filename}"
                )
                break
            except FileNotFoundError:
                # For some reasons, spark logs may appear in the storage not right after execution has stop.
                print(
                    f"Unable to write report for {exp_name}_{i} to the file {report_filename}. "
                    f"Retry {j+1}/{max_retries}."
                )
                time.sleep(5.0)
