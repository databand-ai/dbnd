from dbnd._core.task_build.task_signature import TASK_ID_INVALID_CHAR_REGEX
from targets import target


class TaskRunMetaFiles(object):
    def __init__(self, root):
        self.root = root

    _ARTIFACTS = "artifacts"
    _METRICS = "metrics"
    _META_DATA_FILE_NAME = "meta.yaml"
    _DEFAULT_METRIC_SOURCE = "user"

    def _output(self, *path):
        return target(self.root, *path)

    def get_metric_folder(self, source=None):
        source = source or TaskRunMetaFiles._DEFAULT_METRIC_SOURCE
        return self._output(TaskRunMetaFiles._METRICS, source, "")  # type: DirTarget

    def get_metric_target(self, metric_key, source=None):
        metric_key = TASK_ID_INVALID_CHAR_REGEX.sub("_", metric_key)
        source = source or TaskRunMetaFiles._DEFAULT_METRIC_SOURCE
        return self._output(TaskRunMetaFiles._METRICS, source, metric_key)

    def get_artifact_target(self, name):
        return self._output(TaskRunMetaFiles._ARTIFACTS, name)

    def get_meta_data_file(self):
        return self._output(TaskRunMetaFiles._META_DATA_FILE_NAME)
