"""
This is the scenario to run for manual check on "preview" values.
"""
from dbnd import PipelineTask, output, parameter, task
from targets import Target


@task
def generate_string(value=parameter(default="some_value")[str]):
    return value


class PipelineWithPreview(PipelineTask):
    value_with_preview = output[str]
    value_with_preview_on_click = output[Target]

    def band(self):
        self.value_with_preview = generate_string("value_as_str")
        self.value_with_preview_on_click = generate_string("value_as_object")


class PipelineWithPreviewSameSignature(PipelineTask):
    value_with_preview = output[str]
    value_with_preview_on_click = output[Target]

    def band(self):
        task_result = generate_string()
        self.value_with_preview = task_result
        self.value_with_preview_on_click = task_result
