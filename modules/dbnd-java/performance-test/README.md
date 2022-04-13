# Spark column profiling performance benchmark

## Prerequisites

1. Have JDK8 installed. [SDKMAN](https://sdkman.io/) is preferred.
2. Have python3 installed. No additional libraries are required.

## How to run

3. Prepare good cup of tea or coffee and run `./benchmark.py`.

## Details

### What benchmark script does?

`benchmark.py` will setup all prerequisites and then execute benchmark one-by-one. Steps actually performed:

1. Download and install Spark 2.4.8.
2. Download three datasets from S3: 1GiB, 4GiB, 10GiB.
3. Start spark master and worker.
4. Build jar with benchmark code. Code is located in `../dbnd-examples/src/main/scala/benchmarks/columns`.
5. For each dataset submits each benchmark to the Spark 5 times. Runs count can be configured in script.
6. Measure execution time and print results. We do measure "whole" execution time, from submitting job to completion.
   This adds some burden, but it shouldn't be big.

### Sample datasets

The datasets we are using in benchmark are [Backblaze HDD stats](https://www.backblaze.com/b2/hard-drive-test-data.html)
from various yeats. It is CSV files with huge list of hard drives and their SMART metrics. Most of the values are zeroes
or nulls, so we select columns with meaningful values. Most of these columns are numeric and high-cardinality.

### Benchmark code

We do testing descriptive statistics calculation. This includes: min, max, mean, stddev, non-null counts. We test three
ways of getting those stats: bundled Spark `summary()`, Deequ `ColumnProfiler`, and manual spark aggregations. Proceed
to the actual benchmarking code for details.

### Spark settings

No special settings are applied. This may cause performance degradation, since there's no point in re-shuffling stuff
given we have single worker. Need to discuss it.
