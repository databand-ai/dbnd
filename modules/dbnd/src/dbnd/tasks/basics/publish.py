import logging
import os

from typing import List

from dbnd import output, parameter, task
from targets import Target


logger = logging.getLogger(__name__)


@task(result=List[str])
def publish_results(data=parameter[Target], published=output.folder[Target]):
    if isinstance(data, dict):
        data = data.items()
    for partition in data:
        if isinstance(partition, tuple):
            partition_basename, partition = partition
            partition_basename = str(partition_basename)
            original_ext = os.path.splitext(os.path.basename(partition.path))[1]
            if not os.path.splitext(partition_basename)[1]:
                logger.info("Partition %s doesn't have extension")
                partition_basename += original_ext
        else:
            partition_basename = os.path.basename(partition.path)
        target_path = os.path.join(published.path, partition_basename)
        logger.info("Copying %s to %s", partition, target_path)
        partition.copy(target_path)
    published.mark_success()
    return [str(partition) for partition in data]
