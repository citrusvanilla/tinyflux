"""Defintion of TinyFlux measurement class.

The measurement class provides a convenient interface into a subset of
data points with a common measurement name.  A measurement is analogous to a
table in a traditional RDBMS.

Usage:
    >>> db = TinyFlux(storage=MemoryStorage)
    >>> m = db.measurement("my_measurement")
"""
from __future__ import annotations

from datetime import datetime
from typing import (
    Callable,
    Dict,
    Iterable,
    Generator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from .point import FieldValue, Point
from .queries import MeasurementQuery, Query, SimpleQuery
from .index import Index
from .storages import Storage

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database import TinyFlux  # pragma: no cover


class Measurement:
    """Define the Measurement class.

    Measurement objects are created at runtime when the TinyFlux 'measurement'
    method is invoked.

    Attributes:
        name: Name of the measurement.
        storage: Storage object for the measurement's parent TinyFlux db.
        index: Index object for the measurement's parent TinyFlux db.
    """

    def __init__(
        self,
        name: str,
        db: TinyFlux,
    ) -> None:
        """Initialize a measurement instance.

        Args:
            name: The name of the measurement.
            db: A reference to the database this measurement belongs to.
        """
        self._name = name
        self._db = db

    @property
    def index(self) -> Index:
        """Get the measurement storage instance."""
        return self._db._index

    @property
    def name(self) -> str:
        """Get the measurement name."""
        return self._name

    @property
    def storage(self) -> Storage:
        """Get the measurement storage instance."""
        return self._db._storage

    def __iter__(self) -> Generator:
        """Define the iterator for this class."""
        for item in self._db._storage:
            _measurement = self._db._storage._deserialize_measurement(item)
            if _measurement == self._name:
                yield self._db._storage._deserialize_storage_item(item)

    def __len__(self) -> int:
        """Get total number of points in this measurement."""
        # Check the index first.
        if self._db._auto_index and self._db._index.valid:
            if self.name in self._db._index._measurements:
                return len(self._db._index._measurements[self.name])
            else:
                return 0

        # Otherwise, iterate over storage and increment a counter.
        count = 0

        for item in self._db._storage:
            if self._db._storage._deserialize_measurement(item) == self._name:
                count += 1

        return count

    def __repr__(self) -> str:
        """Get a printable representation of this measurement."""
        if self._db._auto_index and self._db._index.valid:
            if self._name in self._db._index._measurements:
                count = len(self._db._index._measurements[self._name])
            else:
                count = 0

            args = [
                f"name={self.name}",
                f"total={count}",
                f"storage={self._db._storage}",
            ]
        else:
            args = [
                f"name={self.name}",
                f"storage={self._db._storage}",
            ]

        return f'<{type(self).__name__} {", ".join(args)}>'

    def all(self, sorted: bool = True) -> List[Point]:
        """Get all points in this measurement.

        Args:
            sorted: Whether or not to return points in sorted time order.

        Returns:
            A list of points.
        """
        points = list(iter(self))

        if sorted:
            points.sort(key=lambda x: x.time)

        return points

    def contains(self, query: SimpleQuery) -> bool:
        """Check whether the measurement contains a point matching a query.

        Args:
            query: A SimpleQuery.

        Returns:
            True if point found, else False.
        """
        return self._db.contains(query, self._name)

    def count(self, query: SimpleQuery) -> int:
        """Count the points matching a query in this measurement.

        Args:
            query: a SimpleQuery.

        Returns:
            A count of matching points in the measurement.
        """
        return self._db.count(query, self._name)

    def get(self, query: SimpleQuery) -> Optional[Point]:
        """Get exactly one point specified by a query from this measurement.

        Returns None if the point doesn't exist.

        Args:
            query: A SimpleQuery.

        Returns:
            First found Point or None.
        """
        return self._db.get(query, self._name)

    def get_field_keys(self) -> List[str]:
        """Get all field keys for this measurement.

        Returns:
            List of field keys, sorted.
        """
        return self._db.get_field_keys(self._name)

    def get_field_values(self, field_key: str) -> List[FieldValue]:
        """Get field values from this measurement for the specified key.

        Args:
            field_key: The field key to get field values for.

        Returns:
            List of field keys, sorted.
        """
        return self._db.get_field_values(field_key, self._name)

    def get_tag_keys(self) -> List[str]:
        """Get all tag keys for this measurement.

        Returns:
            List of tag keys, sorted.
        """
        return self._db.get_tag_keys(self._name)

    def get_tag_values(self, tag_keys: List[str] = []) -> Dict[str, List[str]]:
        """Get all tag values in the database.

        Args:
            tag_keys: Optional list of tag keys to get associated values for.

        Returns:
            Mapping of tag_keys to associated tag values as a sorted list.
        """
        return self._db.get_tag_values(tag_keys, self._name)

    def get_timestamps(self) -> List[datetime]:
        """Get all timestamps in the database.

        Returns timestamps in order of insertion in the database, as time-aware
        datetime objects with UTC timezone.

        Args:
            measurement: Optional measurement to filter by.

        Returns:
            List of timestamps by insertion order.
        """
        return self._db.get_timestamps(self._name)

    def insert(self, point: Point) -> int:
        """Insert a Point into a measurement.

        If the passed Point has a different measurement value, 'insert' will
        update the measurement value with that of this measurement.

        Args:
            point: A Point object.

        Returns:
            1 if success.

        Raises:
            TypeError if point is not a Point instance.
        """
        return self._db.insert(point, self._name)

    def insert_multiple(self, points: Iterable[Point]) -> int:
        """Insert Points into this measurement.

        If the passed Point has a different measurement value, 'insert' will
        update the measurement value with that of this measurement.

        Args:
            points: An iterable of Point objects.

        Returns:
            The count of inserted points.

        Raises:
            TypeError if point is not a Point instance.
        """
        return self._db.insert_multiple(points, self._name)

    def remove(self, query: SimpleQuery) -> int:
        """Remove Points from this measurement by query.

        This is irreversible.

        Returns:
            The count of removed points.
        """
        return self._db.remove(query, self._name)

    def remove_all(self) -> int:
        """Remove all Points from this measurement.

        This is irreversible.

        Returns:
            The count of removed points.
        """
        return self._db.drop_measurement(self._name)

    def search(self, query: SimpleQuery, sorted: bool = True) -> List[Point]:
        """Get all points specified by a query from this measurement.

        Order is not guaranteed. Returns empty list if no points are found.

        Args:
            query: A SimpleQuery.
            sorted: Whether or not to return points sorted by timestamp.

        Returns:
            A list of found Points.
        """
        return self._db.search(query, self._name, sorted=sorted)

    def select(
        self,
        keys: Union[str, Iterable[str]],
        query: Query,
    ) -> List[Tuple[Union[datetime, str, int, float, None]]]:
        """Get specified attributes from Points specified by a query.

        'keys' should be an iterable of attributres including 'time',
        'measurement', and tag keys and tag values.  Passing 'tags' or 'fields'
        in the 'keys' iterable will not retrieve all tag and/or field values.
        Tag and field keys must be specified individually.

        Args:
            keys: An iterable of Point attributes.
            query: A Query.

        Returns:
            A list of tuples of Point attribute values.
        """
        return self._db.select(keys, query, self._name)

    def update(
        self,
        query: Query,
        time: Union[datetime, Callable[[datetime], datetime], None] = None,
        measurement: Union[str, Callable[[str], str], None] = None,
        tags: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
        fields: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
    ) -> int:
        """Update all matching Points in this measurement with new attributes.

        Args:
            query: A query.
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.

        Returns:
            A count of updated points.
        """
        return self._db.update(
            query, time, measurement, tags, fields, self._name
        )

    def update_all(
        self,
        time: Union[datetime, Callable[[datetime], datetime], None] = None,
        measurement: Union[str, Callable[[str], str], None] = None,
        tags: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
        fields: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
    ) -> int:
        """Update all matching Points in this measurement with new attributes.

        Args:
            query: A query.
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.

        Returns:
            A count of updated points.
        """
        q = MeasurementQuery().noop()

        return self._db.update(q, time, measurement, tags, fields, self._name)
