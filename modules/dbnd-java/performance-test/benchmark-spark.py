#!/usr/bin/env python

import os
import tarfile
import time
import urllib.request

from os.path import exists

import datasets

from datasets import DATASETS, download_data
from stats import print_stats


SPARK_VERSION = "2.4.8"
SPARK_DIR = f"spark-{SPARK_VERSION}-bin-hadoop2.7"
SPARK_DIST = f"{SPARK_DIR}.tgz"

working_dir = datasets.working_dir
spark_exists = exists(os.path.join(working_dir, SPARK_DIR, "sbin", "start-master.sh"))
data_working_dir = datasets.data_working_dir


def install_spark():
    if spark_exists:
        print("Spark already installed")
    else:
        print("Spark is not installed, downloading...")
        spark_dist_url = (
            f"https://archive.apache.org/dist/spark/spark-{SPARK_VERSION}/{SPARK_DIST}"
        )
        spark_dist_path = os.path.join(working_dir, SPARK_DIST)
        urllib.request.urlretrieve(spark_dist_url, spark_dist_path)
        print("Download complete, extracting")

        opened_tar = tarfile.open(spark_dist_path)
        opened_tar.extractall(working_dir)
        os.remove(spark_dist_path)
        print("Spark installed")


install_spark()
download_data()


def is_spark_running():
    import psutil

    master_running = False
    worker_running = False
    for proc in psutil.process_iter():
        try:
            if "java" not in proc.exe():
                continue
            proc_name = " ".join(proc.cmdline())
            if SPARK_DIR in proc_name:
                if "org.apache.spark.deploy.master.Master" in proc_name:
                    master_running = True
                if "org.apache.spark.deploy.worker.Worker" in proc_name:
                    worker_running = True
                if master_running and worker_running:
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


def start_spark():
    start_master_script = os.path.join(working_dir, SPARK_DIR, "sbin/start-master.sh")
    output = os.popen(f"{start_master_script} --host 127.0.0.1")
    output.read()
    result = output.close()
    if result is None:
        print("Master started")
    else:
        print(f"Unable to start spark master")
        return
    start_worker_script = os.path.join(working_dir, SPARK_DIR, "sbin/start-slave.sh")
    output = os.popen(f"{start_worker_script} spark://127.0.0.1:7077")
    output.read()
    result = output.close()
    if result is None:
        print("Worker started")
    else:
        print(f"Unable to start spark worker")


def stop_spark():
    stop_worker_script = os.path.join(working_dir, SPARK_DIR, "sbin/stop-slave.sh")
    output = os.popen(stop_worker_script)
    result = output.close()
    if result is None:
        print("Worker stopped")
    else:
        print(f"Unable to stop spark worker. Maybe they're not running.")
    stop_master_script = os.path.join(working_dir, SPARK_DIR, "sbin/stop-master.sh")
    output = os.popen(stop_master_script)
    result = output.close()
    if result is None:
        print("Master stopped")
    else:
        print(f"Unable to stop spark master. Maybe it's not running")


if is_spark_running():
    print("Spark is running")
else:
    print("Spark is not running, starting...")
    start_spark()


def build_jars():
    print("Building jars...")
    os.chdir(os.path.join(working_dir, ".."))
    build_jars_script = "./gradlew fatJar"
    print(f'Running "{build_jars_script}"')
    result = os.popen(build_jars_script)
    print(result.read())


build_jars()


def submit_spark_job(dataset, benchmark_class):
    print("Submitting spark job")
    submit_script = os.path.join(working_dir, SPARK_DIR, "bin", "spark-submit")
    spark_host = "spark://127.0.0.1:7077"
    jar_path = os.path.join(
        working_dir,
        "..",
        "dbnd-examples",
        "build",
        "libs",
        "dbnd-examples-latest-all.jar",
    )
    dataset_path = os.path.join(data_working_dir, dataset)
    driver_opts = "--driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
    start = time.time()
    output = os.popen(
        f"{submit_script} {driver_opts} --master {spark_host} --class {benchmark_class} {jar_path} {dataset_path}"
    )
    print(output.read())
    elapsed = time.time() - start
    print(f"Elapsed time: {elapsed}")
    return elapsed


BENCHMARKS = [
    "ai.databand.benchmarks.columns.SparkStats",
    "ai.databand.benchmarks.columns.DeequStats",
    "ai.databand.benchmarks.columns.SparkManualStats",
]
RUNS = 5


def run_dataset(dataset):
    results = {}
    for benchmark in BENCHMARKS:
        times = []
        for i in range(RUNS):
            elapsed_time = submit_spark_job(dataset, benchmark)
            times.append(elapsed_time)
        print_stats(times)
        results[benchmark] = times
    return results


def print_results(results):
    for dataset in DATASETS:
        for benchmark in BENCHMARKS:
            times = results[dataset][benchmark]
            print(f"\n{benchmark} on {dataset} times:")
            print_stats(times)


def run_benchmark():
    results = {}
    for dataset in DATASETS:
        results[dataset] = run_dataset(dataset)
    print("\nBenchmark completed.\n")
    print_results(results)


run_benchmark()
