# © Copyright Databand.ai, an IBM Company 2022


# © Copyright Databand.ai, an IBM Company 2022
from targets.marshalling.marshaller_loader import dbnd_package_marshaller
from targets.target_config import FileFormat
from targets.values import ValueTypeLoader, register_value_type


def register_value_types_numpy():
    value_type_ndarray = register_value_type(
        ValueTypeLoader(
            "numpy.ndarray",
            "targets.providers.numpy.numpy_values.NumpyArrayValueType",
            "dbnd",
            type_str_extras=["numpy.ndarray", "ndarray", "np.ndarray"],
        )
    )

    value_type_ndarray.register_marshallers(
        {
            FileFormat.numpy: dbnd_package_marshaller(
                "targets.providers.numpy.numpy_marshaller.NumpyArrayMarshaller"
            ),
            FileFormat.pickle: dbnd_package_marshaller(
                "targets.providers.numpy.numpy_marshaller.NumpyArrayPickleMarshaler"
            ),
        }
    )
