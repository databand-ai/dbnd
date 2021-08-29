from dbnd import PythonTask, parameter, pipeline


"""#### DOC START
from dbnd_spark import SparkConfig


calculate_alpha(
    task_config={
        calculate_alpha.alpha: 0.5,
        calculate_alpha.beta: 0.1,
        calculate_alpha.processName: "calculate_alpha",
    }
)


class calculate_beta(calculate_alpha):
    defaults = {
        calculate_alpha.alpha: 0.4,
        calculate_alpha.beta: 0.2,
        calculate_alpha.processName: "calculate_beta",
    }


calculate_beta(
    override={
        calculate_alpha.alpha: 0.4,
        calculate_alpha.beta: 0.2,
        calculate_alpha.processName: "calculate_beta",
    }
)

"""  #### DOC END
