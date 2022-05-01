"""Defintion of TinyFlux measurement class.

The measurement class provides a convenient interface a the subset of
data points with a common measurement name.  A measurement is analogous to a
table in a traditional RDBMS.

Usage:
    >>> db = TinyFlux(storage=MemoryStorage)
    >>> m = db.measurement("my_measurement")
"""
import copy
from datetime import datetime
from typing import (
    Callable,
    Iterator,
    Iterable,
    Generator,
    List,
    Mapping,
    Optional,
    Union,
)

from .point import Point, validate_tags, validate_fields
from .queries import CompoundQuery, MeasurementQuery, SimpleQuery
from .index import Index
from .storages import Storage


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
        auto_index: bool,
        index_sorter: Callable,
        storage: Storage,
        index: Index,
        name: str,
    ) -> None:
        """Initialize a measurement instance.

        Args:
            auto_index: The index should be automatically managed.
            index_sorter: The ordering function for the index.
            storage: The Storage instance for the parent database.
            index: The Index instance for the parent database.
            name: The name of the measurement.
        """
        self._name = name
        self._storage = storage
        self._index = index
        self._auto_index = auto_index
        self._index_sorter = index_sorter

    @property
    def name(self) -> str:
        """Get the measurement name."""
        return self._name

    @property
    def storage(self) -> Storage:
        """Get the measurement storage instance."""
        return self._storage

    @property
    def index(self) -> Index:
        """Get the measurement storage instance."""
        return self._index

    def __iter__(self) -> Generator:
        """Define the iterator for this class."""
        for item in self._storage:
            _measurement = self._storage._deserialize_measurement(item)
            if _measurement == self._name:
                yield self._storage._deserialize_storage_item(item)

    def __len__(self) -> int:
        """Get total number of points in this measurement."""
        # Check the index first.
        if self._auto_index and self._index.valid:
            if self.name in self._index._measurements:
                return len(self._index._measurements[self.name])
            else:
                return 0

        # Otherwise, iterate over storage and increment a counter.
        count = 0

        def counter(r: Iterator, _, deserialize_measurement: Callable) -> None:
            """Count over an iterator."""
            nonlocal count

            for item in r:
                if deserialize_measurement(item) == self._name:
                    count += 1

            return

        self._search_storage(counter)

        return count

    def __repr__(self) -> str:
        """Get a printable representation of this measurement."""
        if self._auto_index and self._index.valid:
            if self._name in self.index._measurements:
                count = len(self.index._measurements[self._name])
            else:
                count = 0

            args = [
                f"name={self.name}",
                f"total={count}",
                f"storage={self._storage}",
            ]
        else:
            args = [
                f"name={self.name}",
                f"storage={self._storage}",
            ]

        return f'<{type(self).__name__} {", ".join(args)}>'

    def all(self) -> List[Point]:
        """Get all points in this measurement."""
        return list(iter(self))

    def contains(self, query: SimpleQuery) -> bool:
        """Check whether the measurement contains a point matching a query.

        Args:
            query: A SimpleQuery.

        Returns:
            True if point found, else False.
        """
        # Return value.
        contains = False

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            q = MeasurementQuery() == self.name
            rst = self._index.search(q & query)

            if not rst.items:
                return False

            if rst._is_complete:
                return True

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until one match is found."""
                nonlocal contains
                eval_count = 0

                for i, row in enumerate(r):
                    if i not in rst.items:
                        continue

                    if query(deserializer(row)):
                        contains = True
                        break

                    eval_count += 1
                    if eval_count == len(rst.items):
                        break

                return

        # Otherwise, check all points.
        else:

            def searcher(
                r: Iterator,
                deserializer: Callable,
                deserialize_measurement: Callable,
            ) -> None:
                """Search over an iterator until one match is found."""
                nonlocal contains

                for item in r:
                    if deserialize_measurement(item) != self._name:
                        continue

                    if query(deserializer(item)):
                        contains = True
                        break

                return

        self._search_storage(searcher)

        return contains

    def count(self, query: SimpleQuery) -> int:
        """Count the documents matching a query in this measurement.

        Args:
            query: a SimpleQuery.

        Returns:
            A count of matching points in the measurement.
        """
        # Return value.
        count = 0

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            q = MeasurementQuery() == self.name
            rst = self._index.search(q & query)

            if not rst.items:
                return 0

            if rst._is_complete:
                return len(rst.items)

            def counter(r: Iterator, deserializer: Callable, _) -> None:
                """Count over an iterator."""
                nonlocal count
                eval_count = 0

                for i, item in enumerate(r):
                    if i not in rst.items:
                        continue

                    if query(deserializer(item)):
                        count += 1

                    eval_count += 1
                    if eval_count == len(rst.items):
                        break

                return

        # Otherwise, check all points.
        else:

            def counter(
                r: Iterator,
                deserializer: Callable,
                deserialize_measurement: Callable,
            ) -> None:
                """Count over an iterator."""
                nonlocal count

                for item in r:
                    if not deserialize_measurement(item) == self._name:
                        continue

                    if query(deserializer(item)):
                        count += 1

                return

        self._search_storage(counter)

        return count

    def get(self, query: SimpleQuery) -> Optional[Point]:
        """Get exactly one point specified by a query from this measurement.

        Returns None if the point doesn't exist.

        Args:
            query: A SimpleQuery.

        Returns:
            First found Point or None.
        """
        # Return value.
        found_point = None

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            q = MeasurementQuery() == self.name
            rst = self._index.search(q & query)

            if not rst.items:
                return None

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until one match is found."""
                nonlocal found_point
                eval_count = 0

                # Iterate over the storage layer.
                for i, row in enumerate(r):

                    # Not a candidate.
                    if i not in rst.items:
                        continue

                    # No further evaluation necessary.
                    if rst.is_complete:
                        found_point = deserializer(row)
                        return

                    # Further evaluation necessary.
                    _point = deserializer(row)
                    if query(_point):
                        found_point = _point
                        return

                    # Increment eval count.
                    eval_count += 1
                    if eval_count == len(rst.items):
                        break

                # No matches found.
                return

        # Otherwise, search all.
        else:

            def searcher(
                r: Iterator,
                deserializer: Callable,
                deserialize_measurement: Callable,
            ) -> None:
                """Search over an iterator until one match is found."""
                nonlocal found_point

                # Evaluate all points until match.
                for i in r:
                    _measurement = deserialize_measurement(i)
                    if _measurement != self._name:
                        continue

                    _point = deserializer(i)
                    if query(_point):
                        found_point = _point
                        break

                # No matches found.
                return

        self._search_storage(searcher)

        return found_point

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
        # Now, we update the table and add the document
        def inserter(points: List[Point]) -> None:
            """Update function."""
            if not isinstance(point, Point):
                raise TypeError("Data must be a Point instance.")

            if not point.time:
                point.time = datetime.utcnow()

            # Update the measurement name if it doesn't match.
            if point.measurement != self._name:
                point.measurement = self._name

            points.append(point)

            return

        self._insert_point(inserter)

        return 1

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
        # Return value.
        count = 0
        t = datetime.utcnow()

        # Now, we update the table and add the document
        def updater(inp_points: List[Point]):
            """Update function."""
            nonlocal count

            for point in points:
                # Make sure the point implements the ``Mapping`` interface
                if not isinstance(point, Point):
                    raise TypeError("Data must be a Point instance.")

                # Update the measurement name if it doesn't match.
                if point.measurement != self._name:
                    point.measurement = self._name

                if not point.time:
                    point.time = t

                inp_points.append(point)
                count += 1

            return

        self._insert_point(updater)

        return count

    def remove(self, query: SimpleQuery) -> int:
        """Remove Points from this measurement by query.

        This is irreversible.

        Returns:
            The count of removed points.
        """
        filtered_items = set({})
        updated_items = {}
        remaining_items_count = 0

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            # Get indices out of the index.
            q = MeasurementQuery() == self.name
            rst = self.index.search(q & query)

            if not rst.items:
                return 0

            def filter_func(
                r: Iterator,
                temp_memory: List[str],
                serializer: Callable,
                deserializer: Callable,
                deserialize_timestamp: Callable,
                deserialize_measurement: Callable,
            ) -> bool:
                """Search over an iterator and filter matches."""
                nonlocal remaining_items_count
                new_index = 0
                items_filtered = False

                for i, row in enumerate(r):
                    # Not a candidate, keep.
                    if i not in rst.items:
                        temp_memory.append(row)
                        updated_items[i] = new_index
                        new_index += 1
                        remaining_items_count += 1
                        continue

                    # Match needing no further eval, remove.
                    if rst.is_complete:
                        filtered_items.add(i)
                        items_filtered = True
                        continue

                    # Match needing further eval, remove.
                    if query(deserializer(row)):
                        filtered_items.add(i)
                        items_filtered = True
                        continue

                    # Not a match.
                    temp_memory.append(row)
                    updated_items[i] = new_index
                    new_index += 1
                    remaining_items_count += 1
                    continue

                return items_filtered

        # Otherwise, check all storage.
        else:

            def filter_func(
                r: Iterator,
                memory: List[str],
                serializer: Callable,
                deserializer: Callable,
                deserialize_timestamp: Callable,
                deserialize_measurement: Callable,
            ) -> bool:
                """Search over an iterator and filter matches."""
                nonlocal remaining_items_count
                items_filtered = False

                for i, row in enumerate(r):
                    _measurement = deserialize_measurement(row)

                    # Not this measurement, keep.
                    if _measurement != self._name:
                        memory.append(row)
                        remaining_items_count += 1
                        continue

                    # Match, filter.
                    if query(deserializer(row)):
                        filtered_items.add(i)
                        items_filtered = True
                        continue

                    # Not a match.
                    memory.append(row)
                    remaining_items_count += 1

                return items_filtered

        # Pass the filter function to the storage layer.
        self._storage.filter(filter_func, reindex=self._auto_index)

        # We're not auto-indexing, return count of removed items.
        if not self._auto_index:
            return len(filtered_items)

        # No more remaining items, reset index and return count.
        elif not remaining_items_count:
            self._index._reset()
            return len(filtered_items)

        # Index was valid and we removed items, update index and return count.
        elif self._index.valid and filtered_items:
            self._index.remove(filtered_items)
            self._index.update(updated_items)
            return len(filtered_items)

        # Index was valid and no items were removed, return 0.
        elif self._index.valid and not filtered_items:
            return 0

        # Index was invalid, storage is now sorted, build index.
        elif not self._index.valid and filtered_items:
            self._build_index()
            return len(filtered_items)

        # Index was invalid, but no items were removed. Storage is unchanged.
        else:
            return 0

    def remove_all(self) -> int:
        """Remove all Points from this measurement.

        This is irreversible.

        Returns:
            The count of removed points.
        """
        filtered_items = set({})
        updated_items = {}
        remaining_items_count = 0

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            # Get indices out of the index.
            q = MeasurementQuery() == self._name
            rst = self.index.search(q)

            if not rst.items:
                return 0

            def filter_func(
                r: Iterator,
                temp_memory: List[str],
                serializer: Callable,
                deserializer: Callable,
                deserialize_timestamp: Callable,
                deserialize_measurement: Callable,
            ) -> bool:
                """Search over an iterator and filter matches."""
                nonlocal remaining_items_count
                new_index = 0
                items_filtered = False

                for i, row in enumerate(r):
                    # Match.
                    if i in rst.items:
                        filtered_items.add(i)
                        items_filtered = True
                        continue

                    # Not a match.
                    temp_memory.append(row)
                    updated_items[i] = new_index
                    new_index += 1
                    remaining_items_count += 1

                return items_filtered

        # Otherwise, check all storage.
        else:

            def filter_func(
                r: Iterator,
                temp_memory: List[str],
                serializer: Callable,
                deserializer: Callable,
                deserialize_timestamp: Callable,
                deserialize_measurement: Callable,
            ) -> bool:
                """Search over an iterator and filter matches."""
                nonlocal remaining_items_count
                items_filtered = False

                for i, row in enumerate(r):
                    _measurement = deserialize_measurement(row)

                    if _measurement == self._name:
                        filtered_items.add(i)
                        items_filtered = True
                        continue

                    temp_memory.append(row)
                    remaining_items_count += 1

                return items_filtered

        # Pass the filter function to the storage layer.
        self._storage.filter(filter_func, reindex=self._auto_index)

        # We're not auto-indexing, return count of removed items.
        if not self._auto_index:
            return len(filtered_items)

        # No more remaining items, reset index and return count.
        elif not remaining_items_count:
            self._index._reset()
            return len(filtered_items)

        # Index was valid and we removed items, update index and return count.
        elif self._index.valid and filtered_items:
            self._index.remove(filtered_items)
            self._index.update(updated_items)
            return len(filtered_items)

        # Index was valid and no items were removed, return 0.
        elif self._index.valid and not filtered_items:
            return 0

        # Index was invalid, storage is now sorted, build index.
        elif not self._index.valid and filtered_items:
            self._build_index()
            return len(filtered_items)

        # Index was invalid, but no items were removed. Storage is unchanged.
        else:
            return 0

    def search(self, query: SimpleQuery) -> List[Point]:
        """Get all points specified by a query from this measurement.

        Order is not guaranteed. Returns empty list if no points are found.

        Args:
            query: A SimpleQuery.

        Returns:
            A list of found Points.
        """
        # Return value.
        found_points = []

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            # Get candidates from index.
            q = MeasurementQuery() == self.name
            rst = self._index.search(q & query)

            # No candidates -> return None.
            if not rst.items:
                return []

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until all matches are found."""
                eval_count = 0

                for i, row in enumerate(r):
                    if i not in rst.items:
                        continue

                    _point = deserializer(row)

                    if rst.is_complete or query(_point):
                        found_points.append(_point)

                    # Check to see if we need to eval any further.
                    eval_count += 1
                    if eval_count == len(rst.items):
                        break

                return

        # Otherwise, check all points.
        else:

            def searcher(
                r: Iterator,
                deserializer: Callable,
                deserialize_measurement: Callable,
            ) -> None:
                """Search over an iterator until all matches are found."""
                for item in r:
                    _measurement = deserialize_measurement(item)
                    if _measurement != self._name:
                        continue

                    _point = deserializer(item)
                    if query(_point):
                        found_points.append(_point)

                return

        self._search_storage(searcher)

        return found_points

    def update(
        self,
        selector: Optional[SimpleQuery] = None,
        time: Optional[Union[datetime, Callable[[datetime], datetime]]] = None,
        measurement: Optional[Union[str, Callable[[str], str]]] = None,
        tags: Optional[Union[Mapping, Callable[[Mapping], Mapping]]] = None,
        fields: Optional[Union[Mapping, Callable[[Mapping], Mapping]]] = None,
    ) -> int:
        """Update all matching Points in this measurement with new attributes.

        Args:
            selector: A SimpleQuery as a condition, or None to update all.
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.

        Returns:
            A count of updated points.
        """
        # Assert correct arguments.
        if not (time or measurement or tags or fields):
            raise ValueError(
                "Must include time, measurement, tags, and/or fields."
            )

        # Validation.
        if time and not callable(time) and not isinstance(time, datetime):
            raise ValueError("Time must be datetime object.")

        if (
            measurement
            and not callable(measurement)
            and not isinstance(measurement, str)
        ):
            raise ValueError("Measurement must be str.")

        if tags and not callable(tags):
            validate_tags(tags)

        if fields and not callable(fields):
            validate_fields(fields)

        # Return value.
        count = 0

        # Define the function that will perform the update.
        def perform_update(point: Point) -> None:
            """Update points."""
            nonlocal count
            old_point = copy.deepcopy(point)

            if time:
                if callable(time):
                    point.time = time(point.time)
                else:
                    point.time = time

            if measurement:
                if callable(measurement):
                    point.measurement = measurement(point.measurement)
                else:
                    point.measurement = measurement

            if tags:
                if callable(tags):
                    point.tags.update(tags(point.tags))
                else:
                    point.tags.update(tags)

            if fields:
                if callable(fields):
                    point.fields.update(fields(point.fields))
                else:
                    point.fields.update(fields)

            if point != old_point:
                count += 1

            return

        # Update all.
        if not selector:

            def updater(
                r: Iterator,
                temp_memory: List[str],
                serializer: Callable,
                deserializer: Callable,
                _,
                deserialize_measurement: Callable,
            ):
                """Update points."""
                for row in r:

                    _measurement = deserialize_measurement(row)

                    if _measurement != self._name:
                        temp_memory.append(row)
                        continue

                    _point = deserializer(row)
                    perform_update(_point)
                    temp_memory.append(serializer(_point))

                return

        # Update by query.
        elif isinstance(selector, (CompoundQuery, SimpleQuery)):
            # Perform the update operation for documents specified by a query
            _query = selector

            # Use the index.
            if self._auto_index and self._index.valid:

                q = MeasurementQuery() == self.name
                rst = self.index.search(q & _query)

                if not rst.items:
                    return 0

                def updater(
                    r: Iterator,
                    temp_memory: List[str],
                    serializer: Callable,
                    deserializer: Callable,
                    _,
                    deserialize_measurement: Callable,
                ):
                    """Update points."""
                    for i, row in enumerate(r):
                        # Not a query match.
                        if i not in rst.items:
                            temp_memory.append(row)
                            continue

                        _point = deserializer(row)

                        # Query match.
                        if rst.is_complete:
                            perform_update(_point)
                            temp_memory.append(serializer(_point))
                            continue

                        # Incomplete query match.
                        if _query(_point):
                            perform_update(_point)
                            temp_memory.append(serializer(_point))
                            continue

                        # Not a match.
                        temp_memory.append(row)

                    return

            # Otherwise, check all items in storage.
            else:

                def updater(
                    r: Iterator,
                    temp_memory: List[str],
                    serializer: Callable,
                    deserializer: Callable,
                    _,
                    deserialize_measurement: Callable,
                ):
                    """Update points."""
                    for row in r:

                        # Not this measurement.
                        if deserialize_measurement(row) != self._name:
                            temp_memory.append(row)
                            continue

                        _point = deserializer(row)

                        # Query match.
                        if _query(_point):
                            perform_update(_point)
                            temp_memory.append(serializer(_point))
                            continue

                        # Not a query match.
                        temp_memory.append(row)

                    return

        else:
            raise ValueError("Selector must be a query or None.")

        self._storage.update(updater, reindex=self._auto_index)

        # If any item was updated, rebuild the index.
        if self._auto_index and count:
            self._build_index()

        return count

    def _build_index(self):
        """ """
        # Dump index.
        self._index._reset()

        # Build the index.
        for item in self._storage:
            _point = self._storage._deserialize_storage_item(item)
            self._index.insert([_point])

        return

    def _insert_point(self, updater: Callable) -> None:
        """Insert point helper.

        Args:
            updater: Update function.
        """
        # Insert the points into storage.
        new_points: List[Point] = []
        updater(new_points)
        self._storage.append(new_points)

        # Update the index.
        if self._auto_index and self._index.valid:
            if self._storage.index_intact:
                self._index.insert(new_points)
            else:
                self._index.invalidate()

        return

    def _search_storage(self, func: Callable) -> None:
        """Search storage layer helper.

        Args:
            func: A callable that accepts an iterator.

        Returns:
            A list of Points.
        """
        self._storage.search(func)

        return
