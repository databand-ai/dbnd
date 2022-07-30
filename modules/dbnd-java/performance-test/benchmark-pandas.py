#!/usr/bin/env python


# © Copyright Databand.ai, an IBM Company 2022

import glob
import time

import datasets
import pandas as pd

from datasets import DATASETS, download_data
from stats import print_stats


download_data()

working_dir = datasets.working_dir
data_working_dir = datasets.data_working_dir


def read_data(dataset):
    path = f"{data_working_dir}/{dataset}/*.csv"
    files = glob.glob(path)
    df = pd.DataFrame()
    total = len(files)
    print(f"Total files: {total}")
    total = total
    for f in files:
        print(f"Reading {f}, {total} files left")
        csv = pd.read_csv(
            f,
            usecols=[
                "capacity_bytes",
                "smart_1_raw",
                "smart_5_raw",
                "smart_9_raw",
                "smart_194_raw",
                "smart_197_raw",
            ],
        )
        df = df.append(csv)
        total = total - 1
    df.dtypes[0] = "UInt64"
    return df


def run_benchmark(df):
    start = time.time()
    df.describe(include="all")
    elapsed = time.time() - start
    print(f"Elapsed time: {elapsed}")
    return elapsed


RUNS = 5

results = {}

for dataset in DATASETS:
    times = []
    df = read_data(dataset)
    for i in range(RUNS):
        times.append(run_benchmark(df))
    results[dataset] = times

for dataset in DATASETS:
    print(f"Results for dataset {dataset}")
    print_stats(results[dataset])
