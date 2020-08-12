import os
import time
import json
import itertools as it

from .process import process_customer_data

# Pattern for `Spark History` URL.
url_pattern = 'https://p-2fvn1mjb2rs7w.emrappui-prod.us-east-2.amazonaws.com/shs/history/{}/jobs/'

exp_input_keys = [
    'input_file',
    'output_file',
    'to_pandas',
    'with_histograms',
    'sampling_type',
    'sampling_fraction',
    'columns_number_multiplicator'
]


def _update_report(app_id, inputs_shape, input_keys, input_values):
    # Copy spark log files from hdfs file system to local file system for python access.
    os.system(f'hdfs dfs -copyToLocal hdfs:///var/log/spark/apps/{app_id} ./logs/')

    logs = [json.loads(line) for line in open(f'./logs/{app_id}').readlines()]

    job_start_stats = {
        v['Job ID']: [q['Callsite'] for w in v['Stage Infos'] for q in w['RDD Info']][0]
        for v in logs if v['Event'] == 'SparkListenerJobStart'
    }

    job_end_stats = [v for v in logs if v['Event'] == 'SparkListenerJobEnd']

    histograms_time = 0.0
    summary_time = 0.0

    histograms_computation_line = "collect at /home/hadoop/.local/lib/python3.6/site-packages/dbnd_spark/"\
                                  "spark_targets/spark_values.py:314"
    summary_computation_line = "summary at NativeMethodAccessorImpl.java:0"

    for item in job_end_stats:
        if job_start_stats.get(item['Job ID'], '').startswith(histograms_computation_line):
            histograms_time += item['Completion Time']
        if job_start_stats.get(item['Job ID'], '').startswith(summary_computation_line):
            summary_time += item['Completion Time']

    total_time = sum([log['Completion Time'] for log in logs if log['Event'] == 'SparkListenerJobEnd'])

    report_item = {
        'app_id': app_id,
        'url': url_pattern.format(app_id),
        'total_time': total_time,
        'histograms_time': histograms_time,
        'summary_time': summary_time,
        'histograms_inputs_shape': inputs_shape
    }
    report_item.update(dict(zip(input_keys, input_values)))

    return report_item


def generate_reports(exp_name, exp_params):
    max_retries = 3
    report_filename = f'reports/{exp_name}_report.jsonl'

    exp_input_combinations = [exp_params[key] for key in exp_input_keys]

    for i, exp_input_values in enumerate(it.product(*exp_input_combinations)):
        app_id, inputs_shape = process_customer_data(f'{exp_name}_{i}', *exp_input_values)

        for j in range(max_retries):
            try:
                report = _update_report(app_id, inputs_shape, exp_input_keys, exp_input_values)
                with open(report_filename, 'a') as fl:
                    fl.write(json.dumps(report) + '\n')
                print(f'Successfully write report for {exp_name}_{i} to the file {report_filename}')
                break
            except FileNotFoundError:
                # For some reasons, spark logs may appear in the storage not right after execution has stop.
                print(f'Unable to write report for {exp_name}_{i} to the file {report_filename}. '
                      f'Retry {j+1}/{max_retries}.')
                time.sleep(5.0)
