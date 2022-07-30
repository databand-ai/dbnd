# © Copyright Databand.ai, an IBM Company 2022

from targets.pipes.base import (
    InputPipeProcessWrapper,
    IOPipeline,
    OutputPipeProcessWrapper,
)


class GzipPipeline(IOPipeline):

    input = "bytes"
    output = "bytes"

    def __init__(self, compression_level=None):
        self.compression_level = compression_level

    def pipe_reader(self, input_pipe):
        return InputPipeProcessWrapper(["gunzip"], input_pipe)

    def pipe_writer(self, output_pipe):
        args = ["gzip"]
        if self.compression_level is not None:
            args.append("-" + str(int(self.compression_level)))
        return OutputPipeProcessWrapper(args, output_pipe)
