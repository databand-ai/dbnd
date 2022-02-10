# Inspired by an tensorflow tutorial:
#   https://www.tensorflow.org/guide/premade_estimators

import tensorflow as tf

from sklearn.model_selection import train_test_split

from dbnd import ConfigPath, data, output, parameter
from dbnd.tasks import PipelineTask, PythonTask
from dbnd_examples.orchestration.tool_tensorflow import iris


CSV_COLUMN_NAMES = ["SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species"]


class DownloadKeras(PythonTask):
    task_target_date = None
    url = parameter[str]
    downloaded = output.csv

    def run(self):
        tf.keras.utils.get_file(self.downloaded.path, self.url)


class PrepareTestAndValidationData(PythonTask):
    raw_data = data
    validation_size = parameter(default=0.5)

    test = output.csv
    validation = output.csv

    def run(self):
        raw = self.raw_data.read_df()
        test_df, validation_df = train_test_split(raw, test_size=self.validation_size)

        test_df.to_target(self.test, index=False)
        validation_df.to_target(self.validation, index=False)


class TrainIrisModel(PythonTask):
    train_set = data
    test_set = data

    batch_size = parameter[int]
    train_steps = parameter[int]

    model = output.folder
    model_data = output.folder

    def _load_data(self, y_name="Species"):
        train = self.train_set.read_df(names=CSV_COLUMN_NAMES, header=0)
        train_x, train_y = train, train.pop(y_name)

        test = self.test_set.read_df(names=CSV_COLUMN_NAMES, header=0)
        test_x, test_y = test, test.pop(y_name)

        return (train_x, train_y), (test_x, test_y)

    def run(self):
        (train_x, train_y), (test_x, test_y) = self._load_data()
        self.model_data.mkdir()
        classifier = iris.build_classifier(train_x, self.model_data.path)

        classifier.train(
            input_fn=lambda: iris.train_input_fn(train_x, train_y, self.batch_size),
            steps=self.train_steps,
        )

        evaluation = classifier.evaluate(
            input_fn=lambda: iris.eval_input_fn(test_x, test_y, self.batch_size)
        )

        for metric_name in evaluation.keys():
            self.log_metric(metric_name, evaluation[metric_name])

        self.model.mkdir_parent()
        t_model = classifier.export_savedmodel(
            self.get_target(name="tmp/model").path, iris.create_receiver_fn()
        )

        self.model.move_from(t_model)
        self.model_data.mark_success()

        # merged = tf.summary.merge_all()
        # train_writer = tf.summary.FileWriter(FLAGS.summaries_dir + '/train',
        #                                      sess.graph)
        # test_writer = tf.summary.FileWriter(FLAGS.summaries_dir + '/test')


class ValidateIrisModel(PythonTask):
    validation_set = data
    model = data

    report = output.csv

    def run(self):
        data = self.validation_set.read_df(names=CSV_COLUMN_NAMES, header=0)
        predictions = iris.predict(data=data, model_path=self.model.path)
        predictions.to_target(self.report)


class PredictIrisType(PipelineTask):
    batch_size = parameter.default(100)[int]
    train_steps = parameter.default(1000)[int]

    train_set_url = parameter(
        config_path=ConfigPath("dbnd_examples", "iris_train_url")
    )[str]
    test_set_url = parameter(config_path=ConfigPath("dbnd_examples", "iris_test_url"))[
        str
    ]

    report = output

    def band(self):
        train = DownloadKeras(url=self.train_set_url)
        test = DownloadKeras(url=self.test_set_url)

        data = PrepareTestAndValidationData(raw_data=test.downloaded)

        model = TrainIrisModel(
            train_set=train.downloaded,
            test_set=data.test,
            batch_size=self.batch_size,
            train_steps=self.train_steps,
        )

        self.report = ValidateIrisModel(
            model=model.model, validation_set=data.validation
        ).report
