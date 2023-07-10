# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List

import dbnd

from dbnd import PythonTask, output


logger = logging.getLogger(__name__)


class PartitionedDataTask(PythonTask):
    """Create partitioned data"""

    partitioned_data = output.folder.csv.target

    def run(self):
        for i in range(10):
            self.partitioned_data.partition("part-%04d" % i).write(str(i))
        self.partitioned_data.mark_success()


class PartitionedDataReader(PythonTask):
    """Read partitioned data"""

    partitioned_data = dbnd.data[List[str]]
    concat = output.data

    def run(self):
        data = self.partitioned_data
        logger.info("Partitioned data read: %s", data)
        self.concat.write("".join(data))


class ExamplePartitionedDataPipeline(dbnd.PipelineTask):
    """Entry point of partitioned data pipeline"""

    concat = output.data

    def band(self):
        # This is a way to override every output of underneath tasks with custom output location (see "_custom")
        with dbnd.dbnd_config(
            config_values={
                "task": {
                    "task_output_path_format": "{root}/{env_label}/{task_family}{task_class_version}_custom/"
                    "{output_name}{output_ext}/date={task_target_date}"
                }
            }
        ):

            partitioned_data = PartitionedDataTask().partitioned_data
            self.concat = PartitionedDataReader(
                partitioned_data=partitioned_data
            ).concat
