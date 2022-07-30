# © Copyright Databand.ai, an IBM Company 2022

from time import time

from mxnet import autograd, gluon, init
from mxnet.gluon import nn
from mxnet.gluon.data.dataset import ArrayDataset
from mxnet.gluon.data.vision import transforms

from dbnd import data, namespace, output, parameter
from dbnd.tasks import PipelineTask
from dbnd_examples.orchestration.tool_mxnet import (
    DownloadFile,
    MxNetGluonTask,
    fashion_data,
    read_minst,
)


namespace("fashion")


def accuracy(output, label):
    return (output.argmax(axis=1) == label.astype("float32")).mean().asscalar()


def build_lenet_classifier():
    net = nn.Sequential()
    net.add(
        nn.Conv2D(channels=6, kernel_size=5, activation="relu"),
        nn.MaxPool2D(pool_size=2, strides=2),
        nn.Conv2D(channels=16, kernel_size=3, activation="relu"),
        nn.MaxPool2D(pool_size=2, strides=2),
        nn.Flatten(),
        nn.Dense(120, activation="relu"),
        nn.Dense(84, activation="relu"),
        nn.Dense(10),
    )
    net.initialize(init=init.Xavier())
    return net


def forward_backward_path(
    network, loss_function, acc_function, train_data, trainer, batch_size
):
    train_loss, train_acc = 0.0, 0.0
    for data, label in train_data:
        # forward + backward
        with autograd.record():
            output = network(data)
            loss = loss_function(output, label)
        loss.backward()
        # update parameters
        trainer.step(batch_size)
        # calculate training metrics
        train_loss += loss.mean().asscalar()
        train_acc += acc_function(output, label)
    return train_acc, train_loss


class TrainModel(MxNetGluonTask):
    learning_rate = parameter.value(0.1)
    optimizer = parameter.value("sgd")
    epoch_num = parameter.value(10)
    batch_size = 256

    training_data = data
    training_labels = data

    test_data = data
    test_labels = data

    model = output

    def _get_data_loader(self, data, labels):
        # prepare data
        data, label = read_minst(str(data), str(labels), gluon_format=True)
        transform_data = transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize(0.13, 0.31)]
        )
        data_set = ArrayDataset(data, label).transform_first(transform_data)

        return gluon.data.DataLoader(data_set, batch_size=self.batch_size, shuffle=True)

    def get_trainer(self, network):
        return gluon.Trainer(
            network.collect_params(),
            self.optimizer,
            {"learning_rate": self.learning_rate},
        )

    def main(self, ctx):
        # prepare data
        train_dataloader = self._get_data_loader(
            self.training_data, self.training_labels
        )
        test_dataloader = self._get_data_loader(self.test_data, self.test_labels)

        network = build_lenet_classifier()
        trainer = self.get_trainer(network)

        # train
        for epoch in range(self.epoch_num):
            train_loss, train_acc, valid_acc = 0.0, 0.0, 0.0
            tic = time()
            train_acc, train_loss = forward_backward_path(
                network,
                gluon.loss.SoftmaxCrossEntropyLoss(),
                accuracy,
                train_dataloader,
                trainer,
                self.batch_size,
            )

            # calculate validation accuracy
            for data, label in test_dataloader:
                valid_acc += accuracy(network(data), label)

            self.log_metric(
                "epoch",
                (
                    epoch,
                    train_loss / len(train_dataloader),
                    train_acc / len(train_dataloader),
                    valid_acc / len(test_dataloader),
                    time() - tic,
                ),
            )

        network.save_parameters(self.model.path)


class PredictClothingType(PipelineTask):
    model = output

    def band(self):
        train_data = DownloadFile.from_web(fashion_data.train_data).data
        train_labels = DownloadFile.from_web(fashion_data.train_label).data
        test_data = DownloadFile.from_web(fashion_data.test_data).data
        test_labels = DownloadFile.from_web(fashion_data.test_label).data

        model_task = TrainModel(
            training_data=train_data,
            training_labels=train_labels,
            test_data=test_data,
            test_labels=test_labels,
        )
        self.model = model_task.model
