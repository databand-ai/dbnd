# © Copyright Databand.ai, an IBM Company 2022
from targets.marshalling.marshaller_loader import dbnd_package_marshaller
from targets.target_config import FileFormat
from targets.values import ValueTypeLoader, register_value_type


def register_value_types_matplotlib():
    ### Matplotlib

    value_type_matplotlib_figure = register_value_type(
        ValueTypeLoader(
            "matplotlib.figure.Figure",
            "targets.providers.matplotlib.matplotlib_values.MatplotlibFigureValueType",
            "dbnd",
            type_str_extras=["figure.Figure"],
        )
    )

    value_type_matplotlib_figure.register_marshallers(
        {
            FileFormat.png: dbnd_package_marshaller(
                "targets.providers.matplotlib.matplotlib_marshallers.MatplotlibFigureMarshaller"
            ),
            FileFormat.pdf: dbnd_package_marshaller(
                "targets.providers.matplotlib.matplotlib_marshallers.MatplotlibFigureMarshaller"
            ),
            FileFormat.pickle: dbnd_package_marshaller(
                "targets.providers.matplotlib.matplotlib_marshallers.ObjPickleMarshaller"
            ),
        }
    )
