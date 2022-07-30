# © Copyright Databand.ai, an IBM Company 2022

from dbnd import parameter
from targets.types import PathStr


beam_data = parameter.folder_data.with_flag(None)[PathStr]
beam_input = beam_data
beam_output = parameter.output.folder_data.with_flag(None)[PathStr]
