#!/usr/bin/env bash

export DBND__TRACKING=True
alias spark_test="spark-submit --deploy-mode client --master yarn spark_performance_test.py"
export S3_DATA="s3://dbnd-dev-playground/data"

# default histograms (no stats)
spark_test -i ${S3_DATA}/5_million_6bool_5int_5str.parquet -n default_histograms
spark_test -i ${S3_DATA}/stringspp2_5M_rows_100_cols.parquet -n default_histograms
spark_test -i ${S3_DATA}/intspp2_5M_rows_100_cols.parquet -n default_histograms
spark_test -i ${S3_DATA}/boolspp2_5M_rows_100_cols.parquet -n default_histograms

# with stats
spark_test -s -i ${S3_DATA}/5_million_6bool_5int_5str.parquet -n histograms_all
spark_test -s -i ${S3_DATA}/stringspp2_5M_rows_100_cols.parquet -n histograms_all
spark_test -s -i ${S3_DATA}/intspp2_5M_rows_100_cols.parquet -n histograms_all
spark_test -s -i ${S3_DATA}/boolspp2_5M_rows_100_cols.parquet -n histograms_all

# with stats + csv
spark_test -i ${S3_DATA}/5_million_5bool_5int_5str.csv -n default_histograms
spark_test -s -i ${S3_DATA}/5_million_5bool_5int_5str.csv -n histograms_all
export DBND__HISTOGRAMS__SPARK_CACHE_DATAFRAME=True
spark_test -i ${S3_DATA}/5_million_5bool_5int_5str.csv -n dataframe_cache
spark_test -s -i ${S3_DATA}/5_million_5bool_5int_5str.csv -n dataframe_cache
unset DBND__HISTOGRAMS__SPARK_CACHE_DATAFRAME

# with stats + csv + parquet cache
export DBND__HISTOGRAM__SPARK_PARQUET_CACHE_DIR="/tmp/"
spark_test -s -i ${S3_DATA}/5_million_5bool_5int_5str.csv -n parquet_cache
spark_test -s -i ${S3_DATA}/booleans_plusplus_5_million.csv -n parquet_cache
spark_test -s -i ${S3_DATA}/strings_pp_5M_rows_100_cols.csv -n parquet_cache
spark_test -s -i ${S3_DATA}/numspp_5M_rows_100_cols.csv -n parquet_cache
unset DBND__HISTOGRAM__SPARK_PARQUET_CACHE_DIR
