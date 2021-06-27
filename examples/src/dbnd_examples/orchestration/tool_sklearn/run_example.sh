#!/usr/bin/env bash
#docker build is sensitive for location, need to run from project root folder

#sanity
dbnd run dbnd_examples.orchestration.tool_sklearn.train_pipeline.train_model_for_customer --train-model-for-customer-data s3://databand-playground/demo/customer_a.csv --env aws_minikube

# generate data
#dbnd run dbnd_examples.pipelines.generate.generate_partner_data --env

#dbnd run dbnd_examples.orchestration.tool_sklearn.train_pipeline.train_model_for_customer --train-model-for-customer-data example_raw_data/customer_a.csv
#dbnd run dbnd_examples.orchestration.tool_sklearn.train_pipeline.train_model_for_customer --train-model-for-customer-data s3://databand-playground/demo/customer_b.csv --env aws_minikube
#dbnd run dbnd_examples.orchestration.tool_sklearn.train_pipeline.train_model_for_customer --train-model-for-customer-data example_raw_data/customer_b.csv

#dbnd run dbnd_examples.orchestration.tool_sklearn.train_pipeline.train_model_for_customer
#dbnd schedule --name train_model_hourly --cmd "dbnd run dbnd_examples.orchestration.tool_sklearn.train_pipeline.train_model_for_customer --train-model-for-customer-data example_raw_data/customer_a.csv" --start_date 2019-08-25 --schedule-interval "@hourly"
