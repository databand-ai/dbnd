# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.errors import DatabandRuntimeError


def data_frame_dict_value_should_be_dict(target, value):
    return DatabandRuntimeError(
        "Can't dump %s to %s': value should be Mapping[str,DataFrame]"
        % (type(value), target)
    )


def dump_to_multi_target(target, value):
    return DatabandRuntimeError(
        "Can't dump %s to %s': target is multi target (read only)"
        % (type(value), target)
    )


def dump_generator_to_file(target):
    return DatabandRuntimeError(
        "Can't dump generator (multiple parts) to %s': target is not directory" % target
    )


def no_marshaller(target, config, value_type, options_message):
    return DatabandRuntimeError(
        "There is no defined way to read/write value of type '{type}' with file format '{format}'. ".format(
            type=value_type.type, format=config.format
        ),
        help_msg="You can provide the expected format of {target}"
        " using --PARAMETER-target switch, for example --my-input--target csv.  "
        "{options_message}".format(target=target, options_message=options_message),
    )


def no_format(target, options_message):
    return DatabandRuntimeError(
        "Can't read file unknown format. "
        "It's file extension can not be mapped to any of known formats.".format(),
        help_msg="You can "
        "provide the expected format of {target} using --PARAMETER--target switch. "
        "Otherwise you can register new extension via 'register_format/register_compression' or "
        "{options_message} For example: --my-input--target csv.gz".format(
            target=target, options_message=options_message
        ),
    )


def type_without_parse_from_str(value_type):
    return DatabandRuntimeError(
        "Value type {} doesn't support parse_from_str".format(value_type),
        help_msg="Review type implementation and set support_parse_from_str flag to True",
    )


def failed_to_save_value__wrong_type(value, target, expected_type):
    return DatabandRuntimeError(
        "Can not save value at {target}, "
        "expected type is '{expected_type}', got '{value_type}'".format(
            target=target, expected_type=expected_type, value_type=type(value)
        ),
        help_msg="Review type implementation and set support_parse_from_str flag to True",
    )


def target_must_be_local_for_tensorflow_marshalling(target):
    return DatabandRuntimeError(
        "Can not read value of tensorflow model in path {path}! Path must be local!".format(
            path=target.path
        ),
        help_msg="To marshall tensorflow objects you must use 'require_local_access`, e.g.:\n@task("
        "result=output.tfmodel.require_local_access[tf.keras.models.Model])\ndef my_task(p1, p2):...",
    )
