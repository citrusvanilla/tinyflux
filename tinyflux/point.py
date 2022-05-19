"""Defintion of the TinyFlux Point class.

A Point is the data type upon which TinyFlux manages.  It contains the time
data and metadata for an individual observation.  Points are serialized and
deserialized from Storage.  SimpleQuerys act upon individual Points.

A Point is comprised of a timestamp, a measurement, fields, and tags.

Fields contains string/numeric key-values, while tags contain
string/string key-values.  This is enforced upon Point instantiation.

Usage:

>>> from tinyflux import Point
>>> p = Point(
        time=datetime.now(timezone.utc),
        measurement="my measurement",
        fields={"my field": 123.45},
        tags={"my tag key": "my tag value"}
    )

"""

from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Union

TagSet = Dict[str, Optional[str]]
FieldValue = Union[int, float, None]
FieldSet = Dict[str, FieldValue]


def validate_tags(tags: Any) -> None:
    """Validate tags.

    Args:
        tags: The object to validate.

    Raises:
        ValueError: Exception if tags cannot be validated.
    """
    if not isinstance(tags, Mapping):
        raise ValueError("Tag set must be a mapping.")

    # Check keys.
    if not all(isinstance(i, str) for i in tags.keys()):
        raise ValueError("Tag set must contain only string keys.")

    # Check values.
    if not all(i is None or isinstance(i, str) for i in tags.values()):
        raise ValueError("Tag set must contain only string values.")

    return


def validate_fields(fields: Any) -> None:
    """Validate fields.

    Args:
        fields: The object to validate.

    Raises:
        ValueError: Exception if fields cannot be validated.
    """
    if not isinstance(fields, Mapping):
        raise ValueError("Field set must be a mapping.")

    # Check keys.
    if not all(isinstance(i, str) for i in fields):
        raise ValueError("Field set must contain only string keys.")

    # Check values.
    for i in fields.values():
        if i is None:
            continue

        if isinstance(i, bool) or not isinstance(i, (int, float)):
            raise ValueError("Field set must contain only numeric values.")

    return


class Point:
    """Define the Point class.

    This is the only data type that TinyFlux handles directly.  It is composed
    of a timestamp, measurement, tag-set, and field-set.

    Usage:
        >>> p = Point(
                time=datetime.now(timezone.utc),
                measurement="my measurement",
                fields={"my field": 123.45},
                tags={"my tag key": "my tag value"}
            )
    """

    _none_str = "_none"
    default_measurement_name = "_default"
    _valid_kwargs = set(["time", "measurement", "tags", "fields"])
    __slots__ = ("_time", "_measurement", "_tags", "_fields")

    _time: Optional[datetime]
    _measurement: Optional[str]
    _tags: TagSet
    _fields: FieldSet

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Init a Point.

        Attributes:
            time: Timestamp. Defaults to time at instantiation.
            measurement: Measurement. Defaults to "_default".
            tags: Tag set. Defaults to empty set.
            fields: Field set. Defaults to empty set.
        """
        # Test for args.
        if args:
            raise TypeError(
                "Point may contain keyword args for time, "
                "measurement, tags, and fields only."
            )

        if kwargs:
            self._validate_kwargs(kwargs)

            self._time = kwargs.get("time", datetime.now(timezone.utc))
            self._measurement = kwargs.get(
                "measurement", self.default_measurement_name
            )
            self._tags = kwargs.get("tags", {})
            self._fields = kwargs.get("fields", {})
        else:
            self._time = None
            self._measurement = self.default_measurement_name
            self._tags = {}
            self._fields = {}

    @property
    def time(self):
        """Get time."""
        return self._time

    @time.setter
    def time(self, value: Any):
        """Set time."""
        if not isinstance(value, datetime):
            raise ValueError("Time must be datetime object.")
        self._time = value

    @property
    def measurement(self):
        """Get measurement."""
        return self._measurement

    @measurement.setter
    def measurement(self, value: Any):
        """Set measurement."""
        if not isinstance(value, str):
            raise ValueError("Measurement must be a string.")
        self._measurement = value

    @property
    def tags(self):
        """Get tags."""
        return self._tags

    @tags.setter
    def tags(self, value: Any):
        """Set tags."""
        validate_tags(value)
        self._tags = value

    @property
    def fields(self):
        """Get fields."""
        return self._fields

    @fields.setter
    def fields(self, value: Any):
        """Get fields."""
        validate_fields(value)
        self._fields = value

    def __eq__(self, other: Any):
        """Define __eq__.

        Args:
            other: Another Point instance.

        Returns:
            All point attributes are equivalent.
        """
        if isinstance(other, self.__class__):
            return (
                self._time == other._time
                and self._measurement == other._measurement
                and self._tags == other._tags
                and self._fields == other._fields
            )

        return False

    def __repr__(self) -> str:
        """Return printable representation of Point."""
        repr_str = "Point("

        # Add time.
        repr_str += (
            f"time={self._time.isoformat() if self._time else 'None'}, "
        )

        # Add measurement.
        repr_str += f"measurement={self._measurement}"

        # Add tags.
        if self._tags:
            tags_str = "; ".join(
                f"{k}:{str(v)}" for k, v in self._tags.items()
            )
            repr_str += f", tags={tags_str}"

        # Add fields.
        if self._fields:
            tags_str = "; ".join(
                f"{k}:{str(v)}" for k, v in self._fields.items()
            )
            repr_str += f", fields={tags_str}"

        # Add the end.
        repr_str += ")"

        return repr_str

    def _deserialize_from_list(self, row: Sequence[str]) -> "Point":
        """Deserialize a python list of utf-8 strings to a Point.

        Args:
            row: A well-formed row of strings, representing a Point.

        Returns:
            A Point object.

        Raises:
            ValueError: Deserializing encounters a bad type.
            RuntimeError: Deserializing encounters an unexpected column.
        """
        p_time = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
        p_measurement = None if row[1] == self._none_str else row[1]

        p_tags: TagSet = {}
        p_fields: FieldSet = {}

        row_len = len(row)
        i = 2

        # Check for tag key/values.
        while i < row_len and row[i][1] == "t":
            t_key, t_value = row[i][5:], row[i + 1]
            p_tags[t_key] = t_value
            i += 2

        # Check for field key/values.
        while i < row_len:
            f_key, f_value = row[i][7:], row[i + 1]

            # Value is an integer.
            if f_value.isdigit() or (
                f_value[0] == "-" and f_value[1:].isdigit()
            ):
                p_fields[f_key] = int(f_value)
                i += 2
                continue

            # Value is a float.
            try:
                p_fields[f_key] = float(f_value)

            # Value is None.
            except Exception:
                p_fields[f_key] = None

            i += 2

        self._time = p_time
        self._measurement = p_measurement
        self._tags = p_tags
        self._fields = p_fields

        return self

    def _serialize_to_list(self) -> Sequence[Union[str, float, int]]:
        """Serialize a Point to a tuple of strings.

        Returns:
            A well-formed tuple of strings, representing a Point.

        Usage:
            >>> sp = Point()._serialize_to_list()
        """
        t = (
            self._time.replace(tzinfo=None).isoformat()
            if self._time
            else self._none_str
        )
        m = str(self._measurement or self._none_str)
        tags = (
            (f"_tag_{k}", str(v) or self._none_str)
            for k, v in self._tags.items()
        )
        fields = (
            (f"_field_{k}", v or self._none_str)
            for k, v in self._fields.items()
        )

        # Flatten.
        row = (
            t,
            m,
            *(i for p in tags for i in p),
            *(i for p in fields for i in p),
        )

        return row

    def _validate_kwargs(self, kwargs) -> None:
        """Validate args and kwargs.

        Helper function validates types of 'time' and 'measurement' arguments.

        Args:
            args: Reference to Point constructor args.
            kwargs: Reference to Point constructor kwargs.

        Raises:
            TypeError: Bad argument keyword.
            ValueError: Unexpected type encountered.
        """
        # Test for bad kwargs.
        unexpected_kwargs = set(kwargs.keys()) - self._valid_kwargs

        if unexpected_kwargs:
            raise TypeError(
                f"Unexpected kwargs "
                f"{', '.join(sorted(list(unexpected_kwargs)))}"
            )

        # Check time.
        if "time" in kwargs and not isinstance(kwargs["time"], datetime):
            raise ValueError("Time must be datetime object.")

        # Check measurement.
        if "measurement" in kwargs and not isinstance(
            kwargs["measurement"], str
        ):
            raise ValueError("Measurement must be a string.")

        # check
        if "tags" in kwargs:
            validate_tags(kwargs["tags"])

        # Check fields.
        if "fields" in kwargs:
            validate_fields(kwargs["fields"])

        return
