# © Copyright Databand.ai, an IBM Company 2022

import gzip
import struct

import mxnet as mx
import numpy as np

from mxnet import nd
from mxnet.gluon import utils


def read_minst(data_file, label_file, gluon_format=False):
    with gzip.open(label_file, "rb") as fin:
        struct.unpack(">II", fin.read(8))
        label = np.frombuffer(fin.read(), dtype=np.uint8).astype(np.int32)
    with gzip.open(data_file, "rb") as fin:
        struct.unpack(">IIII", fin.read(16))
        data = np.frombuffer(fin.read(), dtype=np.uint8)
        if gluon_format:
            data = data.reshape(len(label), 28, 28, 1)
            data = nd.array(data, dtype=data.dtype)
        else:
            data = data.reshape(len(label), 1, 28, 28).astype(np.float32) / 255
    return data, label


def mnist_to_ndarray_iterator(data_file, label_file, batch_size, shuffle=False):
    data, label = read_minst(data_file, label_file, gluon_format=False)
    return mx.io.NDArrayIter(data, label, batch_size, shuffle)


class FashionMNIST(object):
    namespace = "gluon/dataset/fashion-mnist"

    train_data = (
        utils._get_repo_file_url(namespace, "train-images-idx3-ubyte.gz"),
        "0cf37b0d40ed5169c6b3aba31069a9770ac9043d",
    )
    train_label = (
        utils._get_repo_file_url(namespace, "train-labels-idx1-ubyte.gz"),
        "236021d52f1e40852b06a4c3008d8de8aef1e40b",
    )
    test_data = (
        utils._get_repo_file_url(namespace, "t10k-images-idx3-ubyte.gz"),
        "626ed6a7c06dd17c0eec72fa3be1740f146a2863",
    )
    test_label = (
        utils._get_repo_file_url(namespace, "t10k-labels-idx1-ubyte.gz"),
        "17f9ab60e7257a1620f4ad76bbbaf857c3920701",
    )


class MNIST(object):
    namespace = "gluon/dataset/mnist"

    train_data = (
        utils._get_repo_file_url(namespace, "train-images-idx3-ubyte.gz"),
        "6c95f4b05d2bf285e1bfb0e7960c31bd3b3f8a7d",
    )
    train_label = (
        utils._get_repo_file_url(namespace, "train-labels-idx1-ubyte.gz"),
        "2a80914081dc54586dbdf242f9805a6b8d2a15fc",
    )
    test_data = (
        utils._get_repo_file_url(namespace, "t10k-images-idx3-ubyte.gz"),
        "c3a25af1f52dad7f726cce8cacb138654b760d48",
    )
    test_label = (
        utils._get_repo_file_url(namespace, "t10k-labels-idx1-ubyte.gz"),
        "763e7fa3757d93b0cdec073cef058b2004252c17",
    )


fashion_data = FashionMNIST()
digit_data = MNIST()
