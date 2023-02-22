# © Copyright Databand.ai, an IBM Company 2022

import os

from dbnd import databand_system_path
from dbnd._core.utils.timezone import utcnow
from tests.airflow_tests.scenarios import dbnd_airflow_test_scenarios_path


LOG_DIR = databand_system_path("airflow/logs/scheduler_perf")
run_name = utcnow().strftime("%Y%m%d_%H%M%S")
dag_folder = dbnd_airflow_test_scenarios_path("airflow_big_dag.py")
dag_id = "big_dag_10_10"
log_scheduler = os.path.join(LOG_DIR, "%s.log" % run_name)
log_processor_file = os.path.join(LOG_DIR, "%s.processor.log" % run_name)
