import logging

from dbnd import PythonTask, output, parameter


logger = logging.getLogger()


class GenerateReportToCustomLocation(PythonTask):
    _conf__base_output_path_fmt = (
        "{root}/{env_label}/reports/{name}/"
        "{output_name}{output_ext}/date={task_target_date}"
    )
    name = parameter.value("report")

    report = output[str]

    def run(self):
        logger.info("Going to write to %s", self.report)
        self.report = "Some text in weird location!"
