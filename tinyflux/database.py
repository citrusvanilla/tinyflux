"""The main module of the TinyFlux package, containing the TinyFlux class."""
import copy
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Union,
)

from .measurement import Measurement
from .index import Index
from .point import Point, validate_fields, validate_tags
from .queries import CompoundQuery, MeasurementQuery, SimpleQuery
from .storages import CSVStorage, Storage


class TinyFlux:
    """The TinyFlux class containing the interface for the TinyFlux package.

    A facade singleton for the TinyFlux program.  Manages the lifecycles of
    Storage, Index, and Measurement instances.  Handles Points and Queries.

    TinyFlux will reindex data in memory and at the storage layer by default.
    To turn off this feature, set the value of 'auto_index' to false in the
    constructor keyword arguments.

    TinyFlux will use the CSV store by default.  To use a different store, pass
    a derived Storage subclass to the 'storage' keyword argument of the
    constructor.

    All other args and kwargs are passed to the Storage instance.

    Data Storage Model:
        Data in TinyFlux is represented as Point objects.  These are serialized
        and inserted into the TinyFlux storage layer in append-only fashion,
        providing the lowest-latency write op possible.  This is of primary
        importance for time-series data which can often be written at a high-
        frequency.  The schema of the storage layer is not rigid, allowing for
        variable metadata structures to be stored to the same data store.

    Attributes:
        storage: A reference to the Storage instance.
        index: A reference to the Index instance.

    Usage:
        >>> from tinyflux import TinyFlux
        >>> db = TinyFlux("my_tf_db.csv")
    """

    # The name of the default table.
    default_measurement_name = "_default"

    # The class that will be used by default to create storage instances.
    default_storage_class = CSVStorage

    def __init__(self, *args, **kwargs) -> None:
        """Initialize a new instance of TinyFlux.

        Args:
            auto_index: Reindexing of data will be performed automatically.
            storage: Class of Storage instance.
        """
        # Auto-index.
        self._auto_index = kwargs.pop("auto_index", True)

        # Index sorter.
        self._index_sorter = lambda l: l.sort(key=lambda x: x.time)

        # Init storage.
        storage = kwargs.pop("storage", self.default_storage_class)
        self._storage: Storage = storage(*args, **kwargs)

        # Init index.
        if not isinstance(self._auto_index, bool):
            raise TypeError("'auto_index' must be True/False.")
        if self._auto_index:
            self._storage.index_sorter = self._index_sorter
        self._index: Index = Index(valid=self._storage.index_intact)

        self._measurements: Dict[str, Measurement] = {}
        self._open = True

    @property
    def storage(self) -> Storage:
        """Get a reference to the storage instance."""
        return self._storage

    @property
    def index(self) -> Index:
        """Get a reference to the index instance."""
        return self._index

    def __enter__(self):
        """Use the database as a context manager.

        Using the database as a context manager ensures that the
        'tinyflux.database.tinyflux.close' method is called upon leaving
        the context.
        """
        return self

    def __exit__(self, *args):
        """Close the storage instance when leaving a context."""
        if self._open:
            self.close()

        return

    def __len__(self):
        """Get the number of Points in the storage layer."""
        # If the index is valid, check it.
        if self._auto_index and self._index.valid:
            return len(self.index)

        count = 0

        # Otherwise, iterate over storage and increment a counter.
        def counter(r: Iterator, *args) -> None:
            """Count over an iterator."""
            nonlocal count
            for _ in r:
                count += 1

            return

        self._search_storage(counter)

        return count

    def __iter__(self) -> Iterator[Point]:
        """Return an iterater for all Points in the storage layer."""
        for row in self._storage:
            yield self._storage._deserialize_storage_item(row)

    def __repr__(self):
        """Get a printable representation of the TinyFlux instance."""
        if self._auto_index and self._index.valid:
            args = [
                f"all_points_count={len(self._index)}",
                f"auto_index_ON={self._auto_index}",
                f"index_valid={self._index.valid}",
            ]
        else:
            args = [
                f"auto_index_ON={self._auto_index}",
                f"index_valid={self._index.valid}",
            ]

        return f'<{type(self).__name__} {", ".join(args)}>'

    def all(self) -> List[Point]:
        """Get all data in the storage layer as Points."""
        return list(iter(self))

    def close(self) -> None:
        """Close the database.

        This may be needed if the storage instance used for this database
        needs to perform cleanup operations like closing file handles.

        To ensure this method is called, the tinyflux instance can be used as a
        context manager::

            with TinyFlux('data.csv') as db:
                db.insert(Point())

        Upon leaving this context, the 'close' method will be called.
        """
        self._open = False
        self.storage.close()

        return

    def contains(self, query: Union[CompoundQuery, SimpleQuery]) -> bool:
        """Check whether the database contains a point matching a query.

        Defines a function that iterates over storage items and submits it to
        the storage layer.

        Args:
            query: A SimpleQuery.

        Returns:
            True if point found, else False.
        """
        # Return value.
        contains = False

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            rst = self.index.search(query)

            if not rst._items:
                return False

            if rst._is_complete:
                return True

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until one match is found."""
                nonlocal contains
                eval_count = 0

                for i, row in enumerate(r):
                    if (
                        len(rst._items) < len(self.index)
                        and i not in rst._items
                    ):
                        continue

                    if query(deserializer(row)):
                        contains = True
                        break

                    eval_count += 1
                    if eval_count == len(rst._items):
                        break

                return

        # Otherwise, check all points.
        else:

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until one match is found."""
                nonlocal contains

                for row in r:
                    if query(deserializer(row)):
                        contains = True
                        break

                return

        self._search_storage(searcher)

        return contains

    def count(self, query: Union[CompoundQuery, SimpleQuery]) -> int:
        """Count the documents matching a query in the database.

        Args:
            query: a SimpleQuery.

        Returns:
            A count of matching points in the measurement.
        """
        # Return value.
        count = 0

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            rst = self.index.search(query)

            if not rst._items:
                return 0

            if rst._is_complete:
                return len(rst._items)

            if len(rst._items) < len(self.index):

                def counter(r: Iterator, deserializer: Callable, _) -> None:
                    """Count over an iterator."""
                    nonlocal count
                    eval_count = 0

                    for i, row in enumerate(r):
                        if i not in rst._items:
                            continue

                        # Match found.
                        if query(deserializer(row)):
                            count += 1

                        eval_count += 1
                        if eval_count == len(rst._items):
                            break

                    return

                self._search_storage(counter)

                return count

        # Otherwise, check all points.
        def counter(r: Iterator, deserializer: Callable, _) -> None:
            """Count over an iterator."""
            nonlocal count

            for row in r:
                if query(deserializer(row)):
                    count += 1

            return

        self._search_storage(counter)

        return count

    def drop_measurement(self, name: str) -> int:
        """Drop a specific measurement from the database.

        Args:
            name: The name of the measurement.
        """
        filtered_items = set({})
        updated_items = {}
        remaining_items_count = 0

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            # Get indices out of the index.
            q = MeasurementQuery() == name
            rst = self.index.search(q)

            if not rst._items:
                return 0

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
                new_index = 0

                for i, row in enumerate(r):
                    # Match. Filter.
                    if i in rst._items:
                        filtered_items.add(i)
                        items_filtered = True
                        continue

                    # Not a match. Keep.
                    memory.append(row)
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

                    # Match. Filter.
                    if _measurement == name:
                        filtered_items.add(i)
                        items_filtered = True
                        continue

                    # Not a match. Keep.
                    memory.append(row)
                    remaining_items_count += 1
                    continue

                return items_filtered

        # Pass the filter function to the storage layer.
        self._storage.filter(filter_func, self._auto_index)

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
        else:
            self._build_index()
            return len(filtered_items)

    def drop_measurements(self) -> None:
        """Drop all measurements from the database.

        This removes all Points from the database.

        This is irreversible.
        """
        self._reset_database()

        return

    def get(self, query: Union[CompoundQuery, SimpleQuery]) -> Optional[Point]:
        """Get exactly one point specified by a query from the database.

        Returns None if the point doesn't exist.

        Args:
            query: A SimpleQuery.

        Returns:
            First found Point or None.
        """
        # Result.
        found_point = None

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            # Get indices out of the index.
            rst = self.index.search(query)

            if not rst._items:
                return None

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until one match is found."""
                nonlocal found_point
                eval_count = 0

                # Iterate over the storage layer.
                for i, row in enumerate(r):

                    # Not a candidate.
                    if i not in rst._items:
                        continue

                    # No further evaluation necessary.
                    if rst._is_complete:
                        found_point = deserializer(row)
                        return

                    # Further evaluation necessary.
                    _point = deserializer(row)
                    if query(_point):
                        found_point = _point
                        return

                    # Increment eval count.
                    eval_count += 1
                    if eval_count == len(rst._items):
                        break

                # No matches found.
                return

        # Otherwise, search all.
        else:

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until one match is found."""
                nonlocal found_point

                # Evaluate all points until match.
                for row in r:
                    _point = deserializer(row)
                    if query(_point):
                        found_point = _point
                        return

                # No matches found.
                return

        self._search_storage(searcher)

        return found_point

    def insert(self, point: Point) -> int:
        """Insert a Point into the database.

        Args:
            point: A Point object.

        Returns:
            1 if success.

        Raises:
            TypeError if point is not a Point instance.

        Todo:
            profile the isinstance call.
        """
        if not isinstance(point, Point):
            raise TypeError("Data must be a Point instance.")

        if not point.time:
            point.time = datetime.utcnow()

        def inserter(points: List[Point]) -> None:
            """Update function."""
            points.append(point)

            return

        self._insert_point(inserter)

        return 1

    def insert_multiple(self, points: Iterable[Any]) -> int:
        """Insert Points into the database.

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
        def updater(inp_points: List[Point]) -> None:
            """Update function."""
            nonlocal count

            for point in points:
                if not isinstance(point, Point):
                    raise TypeError("Data must be a Point instance.")

                if not point.time:
                    point.time = t

                inp_points.append(point)
                count += 1

            return

        self._insert_point(updater)

        return count

    def measurement(self, name: str, **kwargs) -> Measurement:
        """Return a reference to a measurement in this database.

        Chained methods will be handled by the Measurement class, and operate
        on the subset of Points belonging to the measurement.

        A measurement does not need to exist in the storage layer for a
        Measurement object to be created.

        Args:
            name: Name of the measurement

        Returns:
            Reference to the measurement.
        """
        # Check _measurements for the name.
        if name in self._measurements:
            return self._measurements[name]

        # Otherwise, create a new Measurement object.
        measurement = Measurement(
            self._auto_index,
            self._index_sorter,
            self._storage,
            self._index,
            name,
            **kwargs,
        )
        self._measurements[name] = measurement

        return measurement

    def measurements(self) -> Set[str]:
        """Get the names of all measurements in the database."""
        # Check the index.
        if self.index.valid:
            return self.index.get_measurement_names()

        # Return value.
        names = set({})

        # Otherwise, check storage.
        def measurement_getter(
            r: Iterator, _: Callable, deserialize_measurement: Callable
        ) -> None:
            """Get measurement names from storage iterator."""
            for item in r:
                names.add(deserialize_measurement(item))

            return

        self._search_storage(measurement_getter)

        return names

    def reindex(self) -> None:
        """Reindex the storage layer and build a new in-memory index.

        Reindexing the storage sorts storage items by timestamp. The Index
        instance is then built while iterating over the storage items.
        """
        # Pass if the index is already valid.
        if self._index.valid:
            print("Index already valid.")
            return

        # Dump index.
        self._index._reset()

        # Check that storage is sorted. If not, sort.
        if not self._storage._is_sorted():
            self._storage.reindex()

        # Build the index.
        for item in self._storage:
            _point = self._storage._deserialize_storage_item(item)
            self._index.insert([_point])

        return

    def remove(self, query: Union[CompoundQuery, SimpleQuery]) -> int:
        """Remove Points from this database by query.

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
            rst = self.index.search(query)

            if not rst._items:
                return 0

            def filter_func(
                r: Iterator,
                temp_memory: List[str],
                _,
                deserializer: Callable,
                *args,
            ) -> bool:
                """Search over an iterator and filter matches."""
                nonlocal remaining_items_count
                items_filtered = False
                new_index = 0

                for i, row in enumerate(r):
                    # Not a candidate. Keep.
                    if i not in rst._items:
                        temp_memory.append(row)
                        updated_items[i] = new_index
                        new_index += 1
                        remaining_items_count += 1
                        continue

                    # Match. Filter
                    if rst._is_complete:
                        filtered_items.add(i)
                        items_filtered = True
                        continue

                    # Candidate.
                    if query(deserializer(row)):
                        filtered_items.add(i)
                        items_filtered = True
                    else:
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
                _,
                deserializer: Callable,
                *args,
            ) -> bool:
                """Search over an iterator and filter matches."""
                nonlocal remaining_items_count
                items_filtered = False

                for i, row in enumerate(r):
                    # Match. Filter.
                    if query(deserializer(row)):
                        filtered_items.add(i)
                        items_filtered = True
                    else:
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
        else:
            self._build_index()
            return len(filtered_items)

    def remove_all(self) -> None:
        """Remove all Points from this database.

        This is irreversible.
        """
        self._reset_database()

        return

    def search(self, query: Union[CompoundQuery, SimpleQuery]) -> List[Point]:
        """Get all points specified by a query.

        Order is guaranteed only if index is valid.

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
            rst = self.index.search(query)

            # No candidates -> return None.
            if not rst._items:
                return []

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until all matches are found."""
                eval_count = 0

                for i, row in enumerate(r):
                    # Not a candidate, skip.
                    if i not in rst._items:
                        continue

                    _point = deserializer(row)

                    # Match or candidate match.
                    if rst._is_complete or query(_point):
                        found_points.append(_point)

                    # Check to see if we need to eval any further.
                    eval_count += 1
                    if eval_count == len(rst._items):
                        break

                return

        # Otherwise, check all points.
        else:

            def searcher(r: Iterator, deserializer: Callable, _) -> None:
                """Search over an iterator until all matches are found."""
                for row in r:
                    _point = deserializer(row)
                    if query(_point):
                        found_points.append(_point)

                return

        self._search_storage(searcher)

        return found_points

    def update(
        self,
        selector: Union[CompoundQuery, SimpleQuery, None] = None,
        time: Union[datetime, Callable, None] = None,
        measurement: Union[str, Callable, None] = None,
        tags: Union[Mapping, Callable, None] = None,
        fields: Union[Mapping, Callable, None] = None,
    ) -> int:
        """Update all matching Points in the database with new attributes.

        Args:
            selector: A query as a condition, or None to update all.
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.

        Returns:
            A count of updated points.

        Todo:
            Update index in a smart way.
        """
        # Assert all arguments.
        if not (time or measurement or tags or fields):
            raise ValueError(
                "Must include time, measurement, tags, and/or fields."
            )

        # Return value.
        count = 0

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

        # Define the function that will perform the update
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

            return point != old_point

        # Update all.
        if selector is None:

            def updater(
                r: Iterator,
                temp_memory: List[str],
                serializer: Callable,
                deserializer: Callable,
                *_,
            ):
                """Update points."""
                updates_performed = False

                for item in r:
                    _point = deserializer(item)

                    u = perform_update(_point)

                    if u:
                        updates_performed = True
                        temp_memory.append(serializer(_point))
                    else:
                        temp_memory.append(item)

                return updates_performed

        # Update by query.
        elif isinstance(selector, (CompoundQuery, SimpleQuery)):
            # Perform the update operation for documents specified by a query
            _query = selector

            # Use the index.
            if self._auto_index and self._index.valid:

                rst = self.index.search(_query)

                if not rst._items:
                    return 0

                def updater(
                    r: Iterator,
                    temp_memory: List[str],
                    serializer: Callable,
                    deserializer: Callable,
                    *_,
                ):
                    """Update points."""
                    updates_performed = False

                    for i, item in enumerate(r):
                        # Not a query match.
                        if i not in rst._items:
                            temp_memory.append(item)
                            continue

                        _point = deserializer(item)

                        # Query match.
                        if rst._is_complete:
                            u = perform_update(_point)

                            if u:
                                updates_performed = True
                                temp_memory.append(serializer(_point))
                            else:
                                temp_memory.append(item)

                            continue

                        # Incomplete query match.
                        if _query(_point):
                            u = perform_update(_point)

                            if u:
                                updates_performed = True
                                temp_memory.append(serializer(_point))
                            else:
                                temp_memory.append(item)

                            continue

                        # Not a match.
                        temp_memory.append(item)

                    return updates_performed

            # Otherwise, check all items in storage.
            else:

                def updater(
                    r: Iterator,
                    temp_memory: List[str],
                    serializer: Callable,
                    deserializer: Callable,
                    *_,
                ):
                    """Update points."""
                    updates_performed = False

                    for item in r:
                        # Cast item to a Point.
                        _point = deserializer(item)

                        # If the query evaluates to true, perform update.
                        if _query(_point):
                            u = perform_update(_point)

                            if u:
                                updates_performed = True
                                temp_memory.append(serializer(_point))
                            else:
                                temp_memory.append(item)

                        # If the query is not True, add the item and continue.
                        else:
                            temp_memory.append(item)

                    return updates_performed

        else:
            raise ValueError("Selector must be a query or None.")

        self._storage.update(updater, self._auto_index)

        # If any item was updated, rebuild the index.
        if self._auto_index and count:
            self._build_index()

        return count

    def _build_index(self):
        """Build the Index instance.

        This assumes the storage layer is sorted, so it should not be accessed
        through public interface.  To check that the storage layer is indeed
        sorted before building the index, use the 'reindex' method.
        """
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

        # Update the index if it is still valid.
        if self._auto_index and self._index.valid:
            if self._storage.index_intact:
                self._index.insert(new_points)
            else:
                self._index.invalidate()

        return

    def _reset_database(self) -> None:
        """Reset TinyFlux and storage."""
        # Write empty list to storage.
        self._storage.write([])

        # Drop measurements.
        self._measurements.clear()

        # Build an index.
        if self._auto_index:
            self._index.build([])

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
