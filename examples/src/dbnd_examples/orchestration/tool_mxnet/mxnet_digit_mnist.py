import mxnet as mx

from dbnd import PipelineTask, data, namespace, output, parameter
from dbnd_examples.data.tool_mxnet import digit_data
from dbnd_examples.src.tool_mxnet.mxnet_task import DownloadFile, MXNetTask


namespace("digits")


def create_mlp_digit_classifier():
    data = mx.sym.var("data")
    # Flatten the data from 4-D shape into 2-D (batch_size, num_channel*width*height)
    data = mx.sym.flatten(data=data)
    # The first fully-connected layer and the corresponding activation function
    fc1 = mx.sym.FullyConnected(data=data, num_hidden=128)
    act1 = mx.sym.Activation(data=fc1, act_type="relu")

    # The second fully-connected layer and the corresponding activation function
    fc2 = mx.sym.FullyConnected(data=act1, num_hidden=64)
    act2 = mx.sym.Activation(data=fc2, act_type="relu")

    # MNIST has 10 classes
    fc3 = mx.sym.FullyConnected(data=act2, num_hidden=10)
    # Softmax with cross entropy loss
    mlp = mx.sym.SoftmaxOutput(data=fc3, name="softmax")
    return mlp


def create_lenet_digit_classifier():
    data = mx.sym.var("data")
    # first conv layer
    conv1 = mx.sym.Convolution(data=data, kernel=(5, 5), num_filter=20)
    tanh1 = mx.sym.Activation(data=conv1, act_type="tanh")
    pool1 = mx.sym.Pooling(data=tanh1, pool_type="max", kernel=(2, 2), stride=(2, 2))
    # second conv layer
    conv2 = mx.sym.Convolution(data=pool1, kernel=(5, 5), num_filter=50)
    tanh2 = mx.sym.Activation(data=conv2, act_type="tanh")
    pool2 = mx.sym.Pooling(data=tanh2, pool_type="max", kernel=(2, 2), stride=(2, 2))
    # first fullc layer
    flatten = mx.sym.flatten(data=pool2)
    fc1 = mx.symbol.FullyConnected(data=flatten, num_hidden=500)
    tanh3 = mx.sym.Activation(data=fc1, act_type="tanh")
    # second fullc
    fc2 = mx.sym.FullyConnected(data=tanh3, num_hidden=10)
    # softmax loss
    lenet = mx.sym.SoftmaxOutput(data=fc2, name="softmax")
    return lenet


class TrainModel(MXNetTask):
    learning_rate = parameter.default(0.1)[float]
    optimizer = parameter.default("sgd")
    epoch_num = parameter[int].default(10)

    training_data = data
    training_labels = data

    test_data = data
    test_labels = data

    model = output

    def main(self, ctx):
        train_iter = self.to_ndarray_iterator(
            self.training_data, self.training_labels, shuffle=True
        )
        val_iter = self.to_ndarray_iterator(self.test_data, self.test_labels)

        # create a trainable module on compute context
        mlp_model = mx.mod.Module(symbol=create_mlp_digit_classifier(), context=ctx)
        mlp_model.fit(
            train_iter,  # train data
            eval_data=val_iter,  # validation data
            optimizer=self.optimizer,  # use SGD to train
            optimizer_params={
                "learning_rate": self.learning_rate
            },  # use fixed learning rate
            eval_metric="acc",  # report accuracy during training
            batch_end_callback=mx.callback.Speedometer(self.batch_size),
            # output progress for each 100 data batches
            num_epoch=self.epoch_num,
        )  # train for at most 10 dataset passes

        # mlp_model.save_checkpoint("mnist", self.epoch_num)
        mlp_model.save_params(self.model.path)


class ValidateModel(MXNetTask):
    model = data
    validation_data = data
    validation_label = data

    report = output

    def main(self, ctx):
        test_iter = self.to_ndarray_iterator(
            self.validation_data, self.validation_label
        )

        loaded = mx.mod.Module(symbol=create_mlp_digit_classifier(), context=ctx)
        loaded.bind(
            data_shapes=test_iter.provide_data, label_shapes=test_iter.provide_label
        )
        loaded.load_params(self.model.path)

        acc = mx.metric.Accuracy()
        mae = mx.metric.MAE()

        loaded.score(test_iter, [acc, mae])
        self.log_metric("accuracy", str(acc.get()[1]))
        self.log_metric("mae", str(mae.get()[1]))

        with self.report.open("w") as fp:
            fp.write(str(acc) + "\n")
            fp.write(str(mae))


class PredictDigits(PipelineTask):
    batch_size = parameter[int].default(100)
    model = output
    report = output

    def band(self):
        train_data = DownloadFile.from_web(digit_data.train_data).data
        train_labels = DownloadFile.from_web(digit_data.train_label).data
        test_data = DownloadFile.from_web(digit_data.test_data).data
        test_labels = DownloadFile.from_web(digit_data.test_label).data

        model_task = TrainModel(
            training_data=train_data,
            training_labels=train_labels,
            test_data=test_data,
            test_labels=test_labels,
        )
        self.report = ValidateModel(
            model=model_task.model,
            validation_data=test_data,
            validation_label=test_labels,
        )

        self.model = model_task.model
