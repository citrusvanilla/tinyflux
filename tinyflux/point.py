"""Defintion of the TinyFlux Point class.

A Point is the data type upon which TinyFlux manages.  It contains the time
data and metadata for an individual observation.  Points are serialized and
deserialized from Storage.  SimpleQuerys act upon individual Points.

A Point is comprised of a timestamp, a measurement, a FieldSet, and a TagSet.

A FieldSet contains string/numeric key-values, while TagSets contain
string/string key-values.  This is enforced upon Point instantiation.

Usage:

>>> from tinyflux import Point
>>> p = Point(
        time=datetime.utcnow(),
        measurement="my measurement",
        fields={"my field": 123.45},
        tags={"my tag key": "my tag value"}
    )

"""

from datetime import datetime
from typing import Mapping, List, Tuple


class TagSet(dict):
    """Definition for the TagSet class.

    Instantiation enforces TagSet dict to be str,str key/value pairs.

    Usage:
        >>> ts = TagSet({"a": "1"})
    """

    def __init__(self, value: Mapping) -> None:
        """Init a TagSet.

        Args:
            value: The key/values as a dict.
        """
        if not isinstance(value, Mapping):
            raise ValueError("Tag set must be a mapping.")

        # Check keys.
        if not all(isinstance(i, str) for i in value.keys()):
            raise ValueError("Tag set must contain only string keys.")

        # Check values.
        if not all(i is None or isinstance(i, str) for i in value.values()):
            raise ValueError("Tag set must contain only string values.")

        super().__init__(value)

    def __setitem__(self, key, value):
        """Define __setitem__.

        Override dict built-in to assert key/val types.

        Args:
            key: String.
            value: String.
        """
        # Check keys.
        if not isinstance(key, str):
            raise ValueError("Tag set must contain only string keys.")

        # Check values.
        if not isinstance(value, str):
            raise ValueError("Tag set must contain only string values.")

        super(TagSet, self).__setitem__(key, value)


class FieldSet(dict):
    """Definition for the FieldSet class.

    Instantiation enforces FieldSet dict to be str,numeric key/value pairs.

    Usage:
        >>> ts = FieldSet({"a": 1})
    """

    def __init__(self, value: Mapping) -> None:
        """Init a FieldSet.

        Args:
            value: The key/values as a dict.
        """
        if not isinstance(value, Mapping):
            raise ValueError("Field set must be a mapping.")

        # Check keys.
        if not all(isinstance(i, str) for i in value.keys()):
            raise ValueError("Field set must contain only string keys.")

        # Check values.
        for i in value.values():
            if i is None:
                continue

            if isinstance(i, bool) or not isinstance(i, (int, float)):
                raise ValueError("Field set must contain only numeric values.")

        super().__init__(value)

    def __setitem__(self, key, value):
        """Define __setitem__.

        Override dict built-in to assert key/val types.

        Args:
            key: String.
            value: Numeric.
        """
        # Check None.
        if value is None:
            super(FieldSet, self).__setitem__(key, value)
            return

        # Check keys.
        if not isinstance(key, str):
            raise ValueError("Field set must contain only string keys.")

        # Check values.
        if not isinstance(value, (int, float)):
            raise ValueError("Field set must contain only numeric values.")

        super(FieldSet, self).__setitem__(key, value)


class Point:
    """Define the Point class.

    This is the only data type that TinyFlux handles directly.  It is composed
    of a timestamp, measurement, tag-set, and field-set.

    Usage:
        >>> p = Point(
                time=datetime.utcnow(),
                measurement="my measurement",
                fields={"my field": 123.45},
                tags={"my tag key": "my tag value"}
            )

    Todo:
        profile and refactor this.
        >>> cProfile.run('Point()')
    """

    _none_str = "_none"
    default_measurement_name = "_default"
    _valid_kwargs = set(["time", "measurement", "tags", "fields"])

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
        self._validate_args(args, kwargs)

        self._time: datetime = kwargs.get("time", datetime.utcnow())
        self._measurement: str = kwargs.get(
            "measurement", self.default_measurement_name
        )
        self._tags: TagSet = (
            TagSet(kwargs.get("tags")) if "tags" in kwargs else {}
        )
        self._fields: FieldSet = (
            FieldSet(kwargs.get("fields")) if "fields" in kwargs else {}
        )

    @property
    def time(self):
        """Get time."""
        return self._time

    @time.setter
    def time(self, value):
        """Set time."""
        self._time = value

    @property
    def measurement(self):
        """Get measurement."""
        return self._measurement

    @measurement.setter
    def measurement(self, value):
        """Set measurement."""
        self._measurement = value

    @property
    def tags(self):
        """Get tags."""
        return self._tags

    @tags.setter
    def tags(self, value):
        """Set tags."""
        self._tags = TagSet(value)

    @property
    def fields(self):
        """Get fields."""
        return self._fields

    @fields.setter
    def fields(self, value):
        """Get fields."""
        self._fields = FieldSet(value)

    def __eq__(self, other):
        """Define __eq__.

        Args:
            other: Another Point instance.

        Returns:
            All point attributes are equivalent.
        """
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__

        return False

    def __repr__(self) -> str:
        """Return printable representation of Point."""
        repr_str = "Point("

        # Add time.
        repr_str += f"time={self._time.isoformat()}, "

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

    def _deserialize(self, row: List[str]) -> "Point":
        """Deserialize a python list of utf-8 strings to a Point.

        Args:
            row: A well-formed row of strings, representing a Point.

        Returns:
            A Point object.

        Raises:
            ValueError: Deserializing encounters a bad type.
            RuntimeError: Deserializing encounters an unexpected column.
        """

        p_time = datetime.fromisoformat(row[0])
        p_measurement = None if row[1] == self._none_str else row[1]

        p_tags, p_fields = {}, {}
        row_len = len(row)
        i = 2

        # Check for tag key/values.
        while i < row_len and row[i][1] == "t":
            t_key, t_value = row[i][5:], row[i + 1]
            p_tags[t_key] = t_value
            i += 2

        # Check for field key/values.
        while i < row_len and row[i][1] == "f":
            f_key, f_value = row[i][7:], row[i + 1]

            try:
                p_fields[f_key] = int(f_value)
                i += 2
                continue
            except Exception:
                pass

            try:
                p_fields[f_key] = float(f_value)
                i += 2
                continue
            except Exception:
                pass

            if f_value == self._none_str:
                p_fields[f_key] = None
                i += 2
                continue

            raise ValueError(f"Invalid field value '{f_value}'")

        if i < row_len:
            raise RuntimeError(f"Unexpected schema for column '{row[i]}'")

        self._time = p_time
        self._measurement = p_measurement
        self._tags = p_tags
        self._fields = p_fields

        return self

    def _serialize(self) -> Tuple[str]:
        """Serialize a Point to a tuple of strings.

        Returns:
            A well-formed tuple of strings, representing a Point.

        Usage:
            >>> sp = Point()._serialize()
        """

        t = self._time.isoformat()
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

    def _validate_args(self, args, kwargs):
        """Validate args and kwargs.

        Helper function validates types of 'time' and 'measurement' arguments.

        Args:
            args: Reference to Point constructor args.
            kwargs: Reference to Point constructor kwargs.

        Raises:
            TypeError: Bad argument keyword.
            ValueError: Unexpected type encountered.
        """
        # Test for args.
        if args:
            raise TypeError(
                "Point may contain keyword args for time, "
                "measurement, tags, and fields only."
            )

        if not kwargs:
            return

        # Test for bad kwargs.
        unexpected_kwargs = set(kwargs.keys()) - self._valid_kwargs

        if unexpected_kwargs:
            raise TypeError(
                f"Unexpected kwargs "
                f"{', '.join(sorted(list(unexpected_kwargs)))}"
            )

        # Check types.
        if "time" in kwargs and not isinstance(kwargs["time"], datetime):
            raise ValueError("Time must be datetime object.")

        # Check tags.
        if "measurement" in kwargs and not isinstance(
            kwargs["measurement"], str
        ):
            raise ValueError("Measurement must be str.")
