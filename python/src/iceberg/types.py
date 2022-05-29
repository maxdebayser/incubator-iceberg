# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Data types used in describing Iceberg schemas

This module implements the data types described in the Iceberg specification for Iceberg schemas. To
describe an Iceberg table schema, these classes can be used in the construction of a StructType instance.

Example:
    >>> str(StructType(
    ...     NestedField(1, "required_field", StringType(), True),
    ...     NestedField(2, "optional_field", IntegerType())
    ... ))
    'struct<1: required_field: optional string, 2: optional_field: optional int>'

Notes:
  - https://iceberg.apache.org/#spec/#primitive-types
"""
from abc import ABC
from typing import ClassVar, Dict, Optional, Tuple

from pydantic import Field, validator

from iceberg.openapi import rest_catalog


class Singleton:
    _instance = None

    def __new__(cls):
        if not isinstance(cls._instance, cls):
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance


class IcebergType(rest_catalog.Type, ABC):
    """Base type for all Iceberg Types"""

    @property
    def string_type(self) -> str:
        return self.__repr__()

    def __str__(self) -> str:
        return self.string_type

    @property
    def is_primitive(self) -> bool:
        return isinstance(self, PrimitiveType)


class PrimitiveType(rest_catalog.PrimitiveType, IcebergType, ABC):
    """Base class for all Iceberg Primitive Types

    Example:
        >>> str(PrimitiveType())
        'PrimitiveType()'
    """

    @property
    def string_type(self) -> str:
        return self.__root__


class FixedType(PrimitiveType):
    """A fixed data type in Iceberg.

    Example:
        >>> FixedType(8)
        FixedType(length=8)
        >>> FixedType(8) == FixedType(8)
        True
    """
    _instances: ClassVar[Dict[int, "FixedType"]] = {}

    @validator('length', check_fields=False)
    def length_must_be_positive(cls, length: int):
        if length <= 0:
            raise ValueError("Length for a FixedType should be positive")
        return length

    def __new__(cls, length: int):
        cls._instances[length] = cls._instances.get(length) or object.__new__(cls)
        return cls._instances[length]

    def __init__(self, length: int):
        self.__root__ = f'fixed[{length}]'
        super(FixedType, self).__init__()


class DecimalType(PrimitiveType):
    """A fixed data type in Iceberg.

    Example:
        >>> DecimalType(32, 3)
        DecimalType(precision=32, scale=3)
        >>> DecimalType(8, 3) == DecimalType(8, 3)
        True
    """

    _instances: ClassVar[Dict[Tuple[int, int], "DecimalType"]] = {}

    @validator('precision', check_fields=False)
    def precision_must_be_positive(cls, precision: int):
        if precision <= 0:
            raise ValueError("Precision for a DecimalType should be positive")
        elif precision > 38:
            raise ValueError("Precision for a DecimalType should be below 38")
        return precision

    @validator('scale', check_fields=False)
    def scale_must_be_positive(cls, scale: int):
        if scale <= 0:
            raise ValueError("Scale for a DecimalType should be positive")
        return scale

    def __new__(cls, precision: int, scale: int):
        key = (precision, scale)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(self, precision: int, scale: int):
        self.__root__ = f'decimal({precision},{scale})'
        super(DecimalType, self).__init__()


class NestedField(rest_catalog.StructField):
    """Represents a field of a struct, a map key, a map value, or a list element.

    This is where field IDs, names, docs, and nullability are tracked.

    Example:
        >>> str(NestedField(
        ...     id=1,
        ...     name='foo',
        ...     type=FixedType(22),
        ...     required=False,
        ... ))
        '1: foo: optional fixed[22]'
        >>> str(NestedField(
        ...     id=2,
        ...     name='bar',
        ...     type=LongType(),
        ...     required=True,
        ...     doc="Just a long"
        ... ))
        '2: bar: required long (Just a long)'
    """
    _instances: ClassVar[Dict[Tuple[bool, int, str, IcebergType, Optional[str]], "NestedField"]] = {}

    type: IcebergType = Field()

    def __new__(
        cls,
        id: int,
        name: str,
        type: IcebergType,
        required: bool = True,
        doc: Optional[str] = None,
    ):
        key = (id, name, type, required, doc)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    @property
    def string_type(self) -> str:
        doc = "" if not self.doc else f" ({self.doc})"
        req = "required" if self.required else "optional"
        return f"{self.id}: {self.name}: {req} {self.type}{doc}"


class StructType(IcebergType, rest_catalog.StructType):
    """A struct type in Iceberg

    Example:
        >>> str(StructType(
        ...     NestedField(1, "required_field", StringType(), True),
        ...     NestedField(2, "optional_field", IntegerType())
        ... ))
        'struct<1: required_field: optional string, 2: optional_field: optional int>'
    """
    _instances: ClassVar[Dict[Tuple[NestedField, ...], "StructType"]] = {}

    def __new__(cls, *fields: NestedField, **kwargs):
        if not fields and "fields" in kwargs:
            fields = kwargs["fields"]
        cls._instances[fields] = cls._instances.get(fields) or object.__new__(cls)
        return cls._instances[fields]

    def __init__(self, *fields: NestedField, **kwargs):
        fields = fields or kwargs.get("fields", [])
        super().__init__(fields=fields)


class ListType(IcebergType, rest_catalog.ListType):
    """A list type in Iceberg

    Example:
        >>> ListType(element_id=3, element=StringType(), element_is_optional=True)
        ListType(element_id=3, element_type=StringType(), element_is_optional=True)
    """
    _instances: ClassVar[Dict[Tuple[bool, int, IcebergType], "ListType"]] = {}

    _element_field = Field(init=False)

    def __new__(
        cls,
        element_id: int,
        element: IcebergType,
        element_required: bool = True,
    ):
        key = (element_id, element, element_required)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(self, **data):
        self._element_field = NestedField(
            name="element",
            id=data["element_id"],
            required=data["element_required"],
            type=data["element"]
        )
        super(ListType, self).__init__(**data)

    @property
    def string_type(self) -> str:
        return f"list<{self.element}>"


class MapType(IcebergType, rest_catalog.MapType):
    """A map type in Iceberg

    Example:
        >>> MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_is_optional=True)
        MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_is_optional=True)
    """
    _instances: ClassVar[Dict[Tuple[int, IcebergType, int, IcebergType, bool], "MapType"]] = {}

    def __new__(
        cls,
        key_id: int,
        key: IcebergType,
        value_id: int,
        value: IcebergType,
        value_required: bool = True,
    ):
        impl_key = (key_id, key, value_id, value, value_required)
        cls._instances[impl_key] = cls._instances.get(impl_key) or object.__new__(cls)
        return cls._instances[impl_key]

    def __init__(self, **data):
        self._key_field = NestedField(
            name="key",
            id=data["key_id"],
            required=True,
            type=data["key"]
        )
        self._value_field = NestedField(
            name="value",
            id=data["value_id"],
            required=data["element_required"],
            type=data["value"]
        )
        super(MapType, self).__init__(**data)


class BooleanType(PrimitiveType, Singleton):
    """A boolean data type in Iceberg can be represented using an instance of this class.

    Example:
        >>> column_foo = BooleanType()
        >>> isinstance(column_foo, BooleanType)
        True
        >>> column_foo
        BooleanType()
    """

    @property
    def string_type(self) -> str:
        return "boolean"


class IntegerType(PrimitiveType, Singleton):
    """An Integer data type in Iceberg can be represented using an instance of this class. Integers in Iceberg are
    32-bit signed and can be promoted to Longs.

    Example:
        >>> column_foo = IntegerType()
        >>> isinstance(column_foo, IntegerType)
        True

    Attributes:
        max (int): The maximum allowed value for Integers, inherited from the canonical Iceberg implementation
          in Java (returns `2147483647`)
        min (int): The minimum allowed value for Integers, inherited from the canonical Iceberg implementation
          in Java (returns `-2147483648`)
    """

    max: ClassVar[int] = 2147483647
    min: ClassVar[int] = -2147483648

    @property
    def string_type(self) -> str:
        return "int"


class LongType(PrimitiveType, Singleton):
    """A Long data type in Iceberg can be represented using an instance of this class. Longs in Iceberg are
    64-bit signed integers.

    Example:
        >>> column_foo = LongType()
        >>> isinstance(column_foo, LongType)
        True
        >>> column_foo
        LongType()
        >>> str(column_foo)
        'long'

    Attributes:
        max (int): The maximum allowed value for Longs, inherited from the canonical Iceberg implementation
          in Java. (returns `9223372036854775807`)
        min (int): The minimum allowed value for Longs, inherited from the canonical Iceberg implementation
          in Java (returns `-9223372036854775808`)
    """

    __root__ = "LongType"

    max: ClassVar[int] = 9223372036854775807
    min: ClassVar[int] = -9223372036854775808

    @property
    def string_type(self) -> str:
        return "long"


class FloatType(PrimitiveType, Singleton):
    """A Float data type in Iceberg can be represented using an instance of this class. Floats in Iceberg are
    32-bit IEEE 754 floating points and can be promoted to Doubles.

    Example:
        >>> column_foo = FloatType()
        >>> isinstance(column_foo, FloatType)
        True
        >>> column_foo
        FloatType()

    Attributes:
        max (float): The maximum allowed value for Floats, inherited from the canonical Iceberg implementation
          in Java. (returns `3.4028235e38`)
        min (float): The minimum allowed value for Floats, inherited from the canonical Iceberg implementation
          in Java (returns `-3.4028235e38`)
    """

    max: ClassVar[float] = 3.4028235e38
    min: ClassVar[float] = -3.4028235e38

    @property
    def string_type(self) -> str:
        return "float"


class DoubleType(PrimitiveType, Singleton):
    """A Double data type in Iceberg can be represented using an instance of this class. Doubles in Iceberg are
    64-bit IEEE 754 floating points.

    Example:
        >>> column_foo = DoubleType()
        >>> isinstance(column_foo, DoubleType)
        True
        >>> column_foo
        DoubleType()
    """

    @property
    def string_type(self) -> str:
        return "double"


class DateType(PrimitiveType, Singleton):
    """A Date data type in Iceberg can be represented using an instance of this class. Dates in Iceberg are
    calendar dates without a timezone or time.

    Example:
        >>> column_foo = DateType()
        >>> isinstance(column_foo, DateType)
        True
        >>> column_foo
        DateType()
    """

    @property
    def string_type(self) -> str:
        return "date"


class TimeType(PrimitiveType, Singleton):
    """A Time data type in Iceberg can be represented using an instance of this class. Times in Iceberg
    have microsecond precision and are a time of day without a date or timezone.

    Example:
        >>> column_foo = TimeType()
        >>> isinstance(column_foo, TimeType)
        True
        >>> column_foo
        TimeType()
    """

    @property
    def string_type(self) -> str:
        return "time"


class TimestampType(PrimitiveType, Singleton):
    """A Timestamp data type in Iceberg can be represented using an instance of this class. Timestamps in
    Iceberg have microsecond precision and include a date and a time of day without a timezone.

    Example:
        >>> column_foo = TimestampType()
        >>> isinstance(column_foo, TimestampType)
        True
        >>> column_foo
        TimestampType()
    """

    @property
    def string_type(self) -> str:
        return "timestamp"


class TimestamptzType(PrimitiveType, Singleton):
    """A Timestamptz data type in Iceberg can be represented using an instance of this class. Timestamptzs in
    Iceberg are stored as UTC and include a date and a time of day with a timezone.

    Example:
        >>> column_foo = TimestamptzType()
        >>> isinstance(column_foo, TimestamptzType)
        True
        >>> column_foo
        TimestamptzType()
    """

    @property
    def string_type(self) -> str:
        return "timestamptz"


class StringType(PrimitiveType, Singleton):
    """A String data type in Iceberg can be represented using an instance of this class. Strings in
    Iceberg are arbitrary-length character sequences and are encoded with UTF-8.

    Example:
        >>> column_foo = StringType()
        >>> isinstance(column_foo, StringType)
        True
        >>> column_foo
        StringType()
    """

    @property
    def string_type(self) -> str:
        return "string"


class UUIDType(PrimitiveType, Singleton):
    """A UUID data type in Iceberg can be represented using an instance of this class. UUIDs in
    Iceberg are universally unique identifiers.

    Example:
        >>> column_foo = UUIDType()
        >>> isinstance(column_foo, UUIDType)
        True
        >>> column_foo
        UUIDType()
    """

    @property
    def string_type(self) -> str:
        return "uuid"


class BinaryType(PrimitiveType, Singleton):
    """A Binary data type in Iceberg can be represented using an instance of this class. Binaries in
    Iceberg are arbitrary-length byte arrays.

    Example:
        >>> column_foo = BinaryType()
        >>> isinstance(column_foo, BinaryType)
        True
        >>> column_foo
        BinaryType()
    """

    @property
    def string_type(self) -> str:
        return "binary"
