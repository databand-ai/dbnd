# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict, List, Optional, Tuple, Union

import attr

from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._vendor._marshmallow.decorators import pre_load
from dbnd._vendor.marshmallow import fields, post_load


@attr.s(frozen=True, auto_attribs=True)
class DataSchemaArgs:
    type: Optional[str] = None
    columns_names: Optional[List[str]] = None
    columns_types: Optional[Dict[str, str]] = None
    shape: Optional[Tuple[Optional[int], Optional[int]]] = attr.ib(
        default=None, converter=lambda field: tuple(field) if field else None
    )
    byte_size: Optional[int] = None

    @shape.validator
    def validate_shape(self, _, val):
        if val and len(val) != 2:
            raise ValueError(
                f"shape should be a tuple of 2 Integers: (records, columns), got {val}"
            )

    def as_dict(self) -> Dict:
        dictted_schema = {
            "type": self.type,
            "columns": self.columns_names,
            "dtypes": self.columns_types,
            "shape": self.shape,
            "size.bytes": self.byte_size,
        }
        return {k: v for k, v in dictted_schema.items() if v is not None}


class StructuredDataSchema(ApiStrictSchema):
    type = fields.String(allow_none=True)
    columns_names = fields.List(
        fields.String(), load_from="columns", dump_to="columns", allow_none=True
    )
    columns_types = fields.Dict(load_from="dtypes", dump_to="dtypes", allow_none=True)
    # Shape: Tuple of ints-> (records, columns) -> fields.Tuple() added in marshmallow 3.0.0.
    shape = fields.List(fields.Integer(allow_none=True), allow_none=True)
    byte_size = fields.Integer(
        load_from="size.bytes", dump_to="size.bytes", allow_none=True
    )

    @pre_load
    def pre_load(self, data: dict) -> dict:
        # Support Snowflake & PostgreSQL data_schemas before dbnd-sdk-0.61.0
        columns_types = data.get("column_types")
        if columns_types:
            data["columns_types"] = columns_types
        return data

    @post_load
    def make_object(self, data) -> DataSchemaArgs:
        return DataSchemaArgs(**data)


def load_data_schema(
    field: Optional[Union[dict, DataSchemaArgs]]
) -> Optional[DataSchemaArgs]:
    if field is None:
        return None

    if isinstance(field, DataSchemaArgs):
        return field
    return StructuredDataSchema().load(field).data
