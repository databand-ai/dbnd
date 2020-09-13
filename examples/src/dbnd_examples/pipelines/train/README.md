#Generate Data

```bash
dbnd run dbnd_examples.example_ml_pipeline.generate.generate_partner_data
```

This pipeline generates data for 3 partners, `a` - csv format, `b`-json format , `c`-csv format:

1. For each partner, 7 days of data under `$DBND_PROJECT\example_raw_data\parthner_<name>` folder.
2. `$DBND_PROJECT\example_raw_data\parthner_a.csv` - a single file to train a model
3. `$DBND_PROJECT\example_raw_data\parthner_b.csv` - a single (bad) file to train a model.
   This file contains random noise in target variable and cause the training pipeline to fail

#Run Scenario
Run basic training pipeline:

```
dbnd run dbnd_examples.example_ml_pipeline.scenario.train_model_for_customer --train-model-for-customer-data example_raw_data/customer_a.csv
```

Run pipeline that fails:

```
dbnd run dbnd_examples.example_ml_pipeline.scenario.train_model_for_customer --train-model-for-customer-data example_raw_data/customer_b.csv
```

Run pipeline with data ingestion. Rund training on a period of 7 days ended on July 11, 2019:

```
dbnd run dbnd_examples.example_ml_pipeline.scenario.train_model_for_customer --period 7d --task-target-date 2019-07-11
```

Train model for a all customers demonstrates how one pipeline can call another pipeline:

```
dbnd run dbnd_examples.example_ml_pipeline.scenario.train_for_all_customer --customer example_raw_data/customer_a.csv,example_raw_data/customer_b.csv
```

Hyperparameter Search example. This demonstrates pipeline composition as well as compute optimization as split_data is called only once for all instances of search.

```
dbnd run dbnd_examples.example_ml_pipeline.scenario.training_with_parameter_search
```

Running Same Pipeline on AWS Kubernetes

```
dbnd run dbnd_examples.example_ml_pipeline.scenario.train_model_for_customer --train-model-for-customer-data s3://databand-playground/demo/customer_b.csv --env aws_minikube
```

Scheduling a pipeline to run hourly

```
dbnd schedule --name train_model_hourly --cmd "dbnd run dbnd_examples.example_ml_pipeline.scenario.train_model_for_customer --train-model-for-customer-data example_raw_data/customer_a.csv" --start_date 2019-08-25 --schedule-interval "@hourly"
```

## Setup minikube demo

Prerequisites:

1.  install minikube
2.  increase minikube vm memory before running the this script i.e. minikube config set memory 4094
3.  install helm - brew install kubernetes-helm

run scripts/deploy.sh to setup minikube and install databand there
run scripts/configure.sh to configure your databand dev project to work with minikube
run scripts/run.sh to generate some load on minikube cluster so
