from pandas import DataFrame

from dbnd import parameter
from dbnd_spark import SparkConfig, SparkTask


class CalculateAlpha(SparkTask):
    alpha = parameter[float]

    def run(self):
        return self.alpha


class PrepareData(SparkTask):
    data = parameter[DataFrame]

    def run(self):
        return self.data


class TestDocTaskConfigurationDefaults:
    def test_doc(self):
        #### DOC START
        class CalculateBeta(CalculateAlpha):
            defaults = {
                SparkConfig.main_jar: "jar2.jar",
                # DataTask.task_env: DataTaskEnv.prod,
                CalculateAlpha.alpha: "0.5",
            }

        class PrepareData2(PrepareData):
            defaults = {
                SparkConfig.main_jar: "jar2.jar",
                # DataTask.task_env: DataTaskEnv.prod,
                CalculateAlpha.alpha: "0.5",
            }

        #### DOC END
