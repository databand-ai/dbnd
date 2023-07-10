# © Copyright Databand.ai, an IBM Company 2022

import pandas as pd
import tensorflow as tf


def train_input_fn(features, labels, batch_size):
    # Convert the inputs to a Dataset.
    dataset = tf.data.Dataset.from_tensor_slices((dict(features), labels))

    # Shuffle, repeat, and batch the dbnd_examples_orchestration.
    dataset = dataset.shuffle(1000).repeat().batch(batch_size)

    # Return the dataset.
    return dataset


def eval_input_fn(features, labels, batch_size):
    features = dict(features)
    if labels is None:
        # No labels, use only features.
        inputs = features
    else:
        inputs = (features, labels)

    # Convert the inputs to a Dataset.
    dataset = tf.data.Dataset.from_tensor_slices(inputs)

    # Batch the dbnd_examples_orchestration
    dataset = dataset.batch(batch_size)
    return dataset


def build_classifier(train, model_dir):
    my_feature_columns = []
    for key in train.keys():
        my_feature_columns.append(tf.feature_column.numeric_column(key=key))

    # Build 2 hidden layer DNN with 10, 10 units respectively.
    classifier = tf.estimator.DNNClassifier(
        feature_columns=my_feature_columns,
        hidden_units=[10, 10],
        n_classes=3,
        optimizer=tf.compat.v1.train.ProximalAdagradOptimizer(
            learning_rate=0.001, l1_regularization_strength=0.01
        ),
        model_dir=model_dir,
        config=tf.estimator.RunConfig().replace(save_summary_steps=10),
    )
    return classifier


def create_receiver_fn():
    feature_spec = {
        "SepalLength": tf.compat.v1.placeholder(
            "float", name="SepalLength", shape=[None]
        ),
        "SepalWidth": tf.compat.v1.placeholder(
            "float", name="SepalWidth", shape=[None]
        ),
        "PetalLength": tf.compat.v1.placeholder(
            "float", name="PetalLength", shape=[None]
        ),
        "PetalWidth": tf.compat.v1.placeholder(
            "float", name="PetalWidth", shape=[None]
        ),
    }
    receiver_fn = tf.estimator.export.build_raw_serving_input_receiver_fn(feature_spec)
    return receiver_fn


def predict(data, model_path):
    graph = tf.Graph()
    with tf.compat.v1.Session(graph=graph) as sess:
        meta_graph_def = tf.compat.v1.saved_model.loader.load(
            sess, [tf.compat.v1.saved_model.tag_constants.SERVING], model_path
        )
        model_signature = meta_graph_def.signature_def["predict"].inputs

        fetch_mapping = {
            sigdef_output: graph.get_tensor_by_name(tnsr_info.name)
            for sigdef_output, tnsr_info in model_signature.outputs.items()
        }

        inputs = {
            graph.get_tensor_by_name(tnsr_info.name): data[sigdef_input]
            for sigdef_input, tnsr_info in model_signature.inputs.items()
        }

        raw_predictions = sess.run(fetch_mapping, feed_dict=inputs)
        predictions_as_dict = {
            fetch_name: list(values) for fetch_name, values in raw_predictions.items()
        }
        return pd.DataFrame(data=predictions_as_dict)
