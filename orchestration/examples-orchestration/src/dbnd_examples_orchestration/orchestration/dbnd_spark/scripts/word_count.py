# © Copyright Databand.ai, an IBM Company 2022

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This file has been modified by databand.ai to include more advance example.

import sys

from operator import add

from pyspark.sql import SparkSession

from dbnd import log_dataframe, task


@task
def word_count(input_path, output_path):
    spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
    lines = spark.read.text(input_path)

    log_dataframe("lines", lines)
    lines = lines.rdd.map(lambda r: r[0])

    log_dataframe("lines_rdd", lines)
    counts = (
        lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    )
    counts.saveAsTextFile(output_path)
    output = counts.collect()
    log_dataframe("output", output)
    for (word, count) in output:
        print("%s: %i" % (word, count))
    # this makes trouble on job submit on databricks!
    # spark.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output>")
        sys.exit(-1)

    word_count(sys.argv[1], sys.argv[2])
