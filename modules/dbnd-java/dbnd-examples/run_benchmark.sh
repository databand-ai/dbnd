#!/usr/bin/env bash

export DBND__TRACKING=True
export DBND__TRACKING__LOG_VALUE_PREVIEW=True
export DBND__CORE__DATABAND_URL="http://127.0.0.1:8080"

export S3_DATA="s3://path-to-playground/data"

for RUN_INPUTS in ${S3_DATA}/5str5num5bool_10Mrows.parquet ${S3_DATA}/10str10num10bool_10Mrows.parquet
do
    for RUN_METHOD in checkDataDeequ profileDataDeequ profileDataDbnd
    do
        dbnd run benchmark.BenchmarkTask \
           --set BenchmarkTask.run_inputs=$RUN_INPUTS \
           --set BenchmarkTask.run_method=$RUN_METHOD \
           --set BenchmarkTask.run_option=includeAll
    done
    dbnd run benchmark.BenchmarkTask \
       --set BenchmarkTask.run_inputs=$RUN_INPUTS \
       --set BenchmarkTask.run_method=profileDataDbnd \
       --set BenchmarkTask.run_option=includeAllNumericOnly
done
