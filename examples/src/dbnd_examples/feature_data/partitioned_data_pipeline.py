import logging

from typing import List

import dbnd

from dbnd import PythonTask, output


logger = logging.getLogger(__name__)


class PartitionedDataTask(PythonTask):
    """ Create partitioned data """

    partitioned_data = output.folder.csv.target

    def run(self):
        for i in range(10):
            self.partitioned_data.partition("part-%04d" % i).write(str(i))
        self.partitioned_data.mark_success()


class PartitionedDataReader(PythonTask):
    """ Read partitioned data """

    partitioned_data = dbnd.data[List[str]]
    concat = output.data

    def run(self):
        data = self.partitioned_data
        logger.info("Partitioned data read: %s", data)
        self.concat.write("".join(data))


class ExamplePartitionedDataPipeline(dbnd.PipelineTask):
    """ Entry point of partitioned data pipeline """

    concat = output.data

    def band(self):
        partitioned_data = PartitionedDataTask().partitioned_data
        self.concat = PartitionedDataReader(partitioned_data=partitioned_data).concat
