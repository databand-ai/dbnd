# © Copyright Databand.ai, an IBM Company 2022

from targets.pipes.base import (
    InputPipeProcessWrapper,
    IOPipeline,
    OutputPipeProcessWrapper,
)


class Bzip2Pipeline(IOPipeline):

    input = "bytes"
    output = "bytes"

    def pipe_reader(self, input_pipe):
        return InputPipeProcessWrapper(["bzcat"], input_pipe)

    def pipe_writer(self, output_pipe):
        return OutputPipeProcessWrapper(["bzip2"], output_pipe)
