# © Copyright Databand.ai, an IBM Company 2022

import mxnet as mx

from mxnet.gluon import utils

from dbnd import PythonTask, output, parameter
from dbnd_examples.orchestration.tool_mxnet import mnist_to_ndarray_iterator


class DownloadFile(PythonTask):
    task_target_date = None
    data_url = parameter[str]
    data_sha = parameter[str]

    data = output

    def run(self):
        utils.download(self.data_url, path=str(self.data), sha1_hash=self.data_sha)

    @classmethod
    def from_web(cls, file_url_and_sha1):
        return cls(data_url=file_url_and_sha1[0], data_sha=file_url_and_sha1[1])


class MXNetTask(PythonTask):
    seed = parameter.value(1)
    batch_size = parameter.value(100)

    def run(self):
        mx.random.seed(42)
        ctx = mx.gpu() if mx.test_utils.list_gpus() else mx.cpu()
        self.main(ctx=ctx)

    def to_ndarray_iterator(self, data_file, label_file, shuffle=False):
        return mnist_to_ndarray_iterator(
            data_file.path, label_file.path, self.batch_size, shuffle
        )


class MxNetGluonTask(MXNetTask):
    pass
