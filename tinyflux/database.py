"""The main module of the TinyFlux package, containing the TinyFlux class."""
import copy
import gc
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
    Tuple,
    Union,
)

from .measurement import Measurement
from .index import Index
from .point import Point, validate_fields, validate_tags
from .queries import CompoundQuery, MeasurementQuery, SimpleQuery, TagQuery
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

    _storage: Storage
    _auto_index: bool
    _index: Index
    _measurements: Dict[str, Measurement]
    _open: bool

    def __init__(self, *args, **kwargs) -> None:
        """Initialize a new instance of TinyFlux.

        Args:
            auto_index: Reindexing of data will be performed automatically.
            storage: Class of Storage instance.
        """
        self._auto_index = kwargs.pop("auto_index", True)

        # Init storage.
        storage = kwargs.pop("storage", self.default_storage_class)
        self._storage = storage(*args, **kwargs)

        # Init index.
        if not isinstance(self._auto_index, bool):
            raise TypeError("'auto_index' must be True/False.")
        self._index = Index(valid=self._storage.index_intact)

        # Init references to measurements.
        self._measurements = {}
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
            return len(self._index)

        # Otherwise, we get it from storage class..
        return len(self._storage)

    def __iter__(self) -> Iterator[Point]:
        """Return an iterater for all Points in the storage layer."""
        for item in self._storage:
            yield self._storage._deserialize_storage_item(item)

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
        return self._storage.read()

    def close(self) -> None:
        """Close the database.

        This may be needed if the storage instance used for this database
        needs to perform cleanup operations like closing file handles.

        To ensure this method is called, the tinyflux instance can be used as a
        context manager:

        >>> with TinyFlux('data.csv') as db:
                db.insert(Point())

        Upon leaving this context, the 'close' method will be called.
        """
        self._open = False
        self._storage.close()

        return

    def contains(
        self, query: Union[CompoundQuery, SimpleQuery], measurement: str = None
    ) -> bool:
        """Check whether the database contains a point matching a query.

        Defines a function that iterates over storage items and submits it to
        the storage layer.

        Args:
            query: A SimpleQuery.

        Returns:
            True if point found, else False.
        """
        # Return value.
        use_index = self._auto_index and self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No candidates from the index.
            if not index_rst._items:
                return False

            # Candidates and no further evaluation necessary.
            if index_rst._is_complete:
                return True

            # Candidates needing evaluation, but it's all of them.
            if len(index_rst._items) == len(self._index):
                use_index = False

        if use_index:
            j = 0

            for i, item in enumerate(self._storage):

                # Not a candidate.
                if i != index_rst._items[j]:
                    continue

                # Candidate, evaluate.
                if query(self._storage._deserialize_storage_item(item)):
                    return True

                j += 1

                # If we are out of candidates, break.
                if j == len(index_rst._items):
                    return False

        else:

            for item in self._storage:
                if (
                    measurement
                    and self._storage._deserialize_measurement(item)
                    != measurement
                ):
                    continue

                if query(self._storage._deserialize_storage_item(item)):
                    return True

            return False

    def count(
        self, query: Union[CompoundQuery, SimpleQuery], measurement: str = None
    ) -> int:
        """Count the documents matching a query in the database.

        Args:
            query: a SimpleQuery.
            measurement: Optional measurement name to query by.

        Returns:
            A count of matching points in the measurement.
        """
        # Return value.
        count = 0

        use_index = self._auto_index and self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No candidates from the index.
            if not index_rst._items:
                return 0

            # Candidates and no further evaluation necessary.
            if index_rst._is_complete:
                return len(index_rst._items)

            # Candidates needing evaluation, but it's all of them.
            if len(index_rst._items) == len(self._index):
                use_index = False

        # Search with help of index.
        if use_index:
            j = 0

            for i, item in enumerate(self._storage):
                # Not a candidate.
                if i != index_rst._items[j]:
                    continue

                # Candidate, evaluate.
                if query(self._storage._deserialize_storage_item(item)):
                    count += 1

                j += 1

                # If we are out of candidates, break.
                if j == len(index_rst._items):
                    break

        # Search without help of index.
        else:

            for item in self._storage:
                if (
                    measurement
                    and not self._storage._deserialize_measurement(item)
                    == measurement
                ):
                    continue

                if query(self._storage._deserialize_storage_item(item)):
                    count += 1

        return count

    def drop_measurement(self, name: str) -> int:
        """Drop a specific measurement from the database.

        If 'auto-index' is True, the storage layer will be sorted after
        this function is run, and a new index will be built.

        Args:
            name: The name of the measurement.

        Returns:
            The count of removed items.
        """
        assert self._storage.can_write

        if name in self._measurements:
            del self._measurements[name]

        return self.remove(MeasurementQuery() == name, name)

    def drop_measurements(self) -> None:
        """Drop all measurements from the database.

        This removes all Points from the database.

        This is irreversible.
        """
        assert self._storage.can_write

        self._reset_database()

        return

    def get(
        self, query: Union[CompoundQuery, SimpleQuery], measurement: str = None
    ) -> Optional[Point]:
        """Get exactly one point specified by a query from the database.

        Returns None if the point doesn't exist.

        Args:
            query: A SimpleQuery.

        Returns:
            First found Point or None.
        """
        use_index = self._auto_index and self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No candidates from the index.
            if not index_rst._items:
                return None

            # Candidates, but it's all of them.
            if len(index_rst._items) == len(self._index):
                use_index = False

        if use_index:

            j = 0

            # Iterate over the storage layer.
            for i, item in enumerate(self._storage):

                # Not a candidate.
                if i != index_rst._items[j]:
                    continue

                # Candidate, no further evaluation necessary.
                if index_rst._is_complete:
                    return self._storage._deserialize_storage_item(item)

                # Candidate, further evaluation necessary.
                _point = self._storage._deserialize_storage_item(item)
                if query(_point):
                    return _point

                j += 1

                # If we are out of candidates, break.
                if j == len(index_rst._items):
                    return None

        else:

            # Evaluate all points until match.
            for item in self._storage:

                if (
                    measurement
                    and self._storage._deserialize_measurement(item)
                    != measurement
                ):
                    continue

                _point = self._storage._deserialize_storage_item(item)
                if query(_point):
                    return _point

            # No matches found.
            return None

    def insert(self, point: Point, measurement: str = None) -> int:
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
        assert self._storage.can_write

        if not isinstance(point, Point):
            raise TypeError("Data must be a Point instance.")

        # Add time if not exists.
        if not point.time:
            point.time = datetime.utcnow()

        # Update the measurement name if it doesn't match.
        if measurement and point.measurement != measurement:
            point.measurement = measurement

        def inserter(points: List[Point]) -> None:
            """Update function."""
            points.append(point)

            return

        self._insert_point(inserter)

        return 1

    def insert_multiple(self, points: Iterable[Any], measurement=None) -> int:
        """Insert Points into the database.

        Args:
            points: An iterable of Point objects.

        Returns:
            The count of inserted points.

        Raises:
            TypeError if point is not a Point instance.
        """
        assert self._storage.can_write

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

                # Update the measurement name if it doesn't match.
                if measurement and point.measurement != measurement:
                    point.measurement = measurement

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
            name,
            self,
            **kwargs,
        )
        self._measurements[name] = measurement

        return measurement

    def measurements(self) -> Set[str]:
        """Get the names of all measurements in the database."""
        # Check the index.
        if self._index.valid:
            return self._index.get_measurement_names()

        # Return value.
        names = set({})

        # Otherwise, check storage.
        for item in self._storage:
            names.add(self._storage._deserialize_measurement(item))

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

    def remove(
        self, query: Union[CompoundQuery, SimpleQuery], measurement=None
    ) -> int:
        """Remove Points from this database by query.

        This is irreversible.

        Returns:
            The count of removed points.
        """
        assert self._storage.can_write

        use_index = self._auto_index and self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No candidates from the index.
            if not index_rst._items:
                return 0

            # Candidates, but it's all of them.
            if len(index_rst.items) == len(self._index):
                use_index = False

        filtered_items = set({})
        updated_items = {}
        temp_memory = []
        new_index = 0

        if use_index:

            j = 0

            for i, item in enumerate(self._storage):
                # No more candidates or item is not a candidate.
                if j == len(index_rst._items) or i != index_rst._items[j]:
                    temp_memory.append(item)
                    updated_items[i] = new_index
                    new_index += 1
                    continue

                # Candidate and no further evaluation necessary.
                if index_rst._is_complete:
                    filtered_items.add(i)

                # Candidate, evaluation is True.
                elif query(self._storage._deserialize_storage_item(item)):
                    filtered_items.add(i)

                # Candidate, evaluation is False.
                else:
                    temp_memory.append(item)
                    updated_items[i] = new_index
                    new_index += 1

                j += 1

        else:

            for i, item in enumerate(self._storage):
                if measurement:
                    _measurement = self._storage._deserialize_measurement(item)

                    # Not this measurement, keep.
                    if _measurement != measurement:
                        temp_memory.append(item)
                        continue

                # Match, filter.
                if query(self._storage._deserialize_storage_item(item)):
                    filtered_items.add(i)

                # Not a match, keep.
                else:
                    temp_memory.append(item)

                    # Update new index.
                    if self._auto_index:
                        updated_items[i] = new_index
                        new_index += 1

        # No items removed.
        if not len(filtered_items):
            del temp_memory
            gc.collect()

            return 0

        # No items remaining.
        if not temp_memory:
            self._reset_database()
            return len(filtered_items)

        # Index was invalid and we need to reindex.
        if self._auto_index and not self._index.valid:
            temp_memory.sort(
                key=lambda x: self._storage._deserialize_timestamp(x)
            )
            self._storage._write(temp_memory, True)

        # Write memory to storage.
        self._storage._write(temp_memory, self.index.valid)

        # Update index.
        if self._auto_index:
            self._filter_index(len(temp_memory), filtered_items, updated_items)

        # Clean up temp memory.
        del temp_memory
        gc.collect()

        return len(filtered_items)

    def remove_all(self) -> None:
        """Remove all Points from this database.

        This is irreversible.
        """
        assert self._storage.can_write

        self._reset_database()

        return

    def search(
        self, query: Union[CompoundQuery, SimpleQuery], measurement=None
    ) -> List[Point]:
        """Get all points specified by a query.

        Order is guaranteed only if index is valid.

        Args:
            query: A SimpleQuery.

        Returns:
            A list of found Points.
        """
        use_index = self._auto_index and self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if self._auto_index and self._index.valid:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No candidates from the index.
            if not index_rst._items:
                return []

            # Candidates, but it's all of them.
            if len(index_rst._items) == len(self._index):
                use_index = False

        # Return value.
        found_points = []

        # Search using help of index.
        if use_index:
            j = 0

            for i, item in enumerate(self._storage):
                # Not a candidate, skip.
                if i != index_rst._items[j]:
                    continue

                _point = self._storage._deserialize_storage_item(item)

                # Match or candidate match.
                if index_rst._is_complete or query(_point):
                    found_points.append(_point)

                j += 1

                # If we are out of candidates, break.
                if j == len(index_rst._items):
                    return found_points

        # Search without index.
        else:

            for item in self._storage:
                if (
                    measurement
                    and self._storage._deserialize_measurement(item)
                    != measurement
                ):
                    continue

                _point = self._storage._deserialize_storage_item(item)
                if query(_point):
                    found_points.append(_point)

            return found_points

    def update(
        self,
        query: Union[CompoundQuery, SimpleQuery],
        time: Union[datetime, Callable, None] = None,
        measurement: Union[str, Callable, None] = None,
        tags: Union[Mapping, Callable, None] = None,
        fields: Union[Mapping, Callable, None] = None,
        _measurement: str = None,
    ) -> int:
        """Update all matching Points in the database with new attributes.

        Args:
            query: A query as a condition.
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.

        Returns:
            A count of updated points.

        Todo:
            Update index in a smart way.
        """
        return self._update_helper(
            query, time, measurement, tags, fields, _measurement
        )

    def update_all(
        self,
        time: Union[datetime, Callable, None] = None,
        measurement: Union[str, Callable, None] = None,
        tags: Union[Mapping, Callable, None] = None,
        fields: Union[Mapping, Callable, None] = None,
    ) -> int:
        """Update all points in the database with new attributes.

        Args:
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.

        Returns:
            A count of updated points.

        Todo:
            Update index in a smart way.
        """
        return self._update_helper(None, time, measurement, tags, fields, None)

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

    def _filter_index(
        self,
        remaining_items_count: int,
        filtered_items: Set,
        updated_items: Dict[int, int],
    ) -> None:
        """Filter the Index.

        A helper the remove items and then update remaining items in the Index.

        Args:
            remaining_items_count: Count of items remaining the storage layer.
            filtered_items: Items set for removal from storage.
            updated_items: A mapping of old indices to update indices.
        """
        # No more remaining items, reset index and return count.
        if not remaining_items_count:
            self._index._reset()
            return

        # Index was valid and we removed items, update index and return count.
        elif self._index.valid and filtered_items:
            self._index.remove(filtered_items)
            self._index.update(updated_items)
            return

        # Index was valid and no items were removed.
        elif self._index.valid and not filtered_items:
            return

        # Index was invalid, storage is now sorted, build index.
        else:
            self._build_index()
            return

    def _generate_updater(
        self, query=None, time=None, measurement=None, tags=None, fields=None
    ) -> Callable:
        """Generate a routine that updates a Point with new attributes.

        Performs validation of attribute arguments.

        The update function will also validate after update.  This makes this
        routine quite slow.

        Args:
            query: A query to match items on.
            time: The time update.
            measurement: The measurement update.
            tags: The tags update.
            fields: The fields update.

        Returns:
            A function that updates a Point's attributes.
        """
        if query and not isinstance(query, (SimpleQuery, CompoundQuery)):
            raise ValueError("Argument 'query' must be a TinyFlux Query.")

        # Assert all arguments.
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
            raise ValueError("Measurement must be a string")

        if tags and not callable(tags):
            validate_tags(tags)

        if fields and not callable(fields):
            validate_fields(fields)

        # Define the update function.
        def perform_update(point: Point) -> Tuple[bool, bool]:
            """Update points."""
            old_point = copy.deepcopy(point)

            if time:
                if callable(time):
                    point.time = time(point.time)
                    if not isinstance(point.time, datetime):
                        raise ValueError(
                            "Time must update to a datetime object."
                        )
                else:
                    point.time = time

            if measurement:
                if callable(measurement):
                    point.measurement = measurement(point.measurement)
                    if not isinstance(point.measurement, str):
                        raise ValueError(
                            "Measurement must update to a string."
                        )
                else:
                    point.measurement = measurement

            if tags:
                if callable(tags):
                    point.tags.update(tags(point.tags))
                    validate_tags(point.tags)
                else:
                    point.tags.update(tags)

            if fields:
                if callable(fields):
                    point.fields.update(fields(point.fields))
                    validate_fields(point.fields)
                else:
                    point.fields.update(fields)

            return point != old_point, point.time != old_point.time

        return perform_update

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

        # Invalidate index.
        if not self._auto_index and self._index.valid:
            self._index.invalidate()

        return

    def _reset_database(self) -> None:
        """Reset TinyFlux and storage."""
        # Write empty list to storage.
        self._storage.reset()

        # Drop measurements.
        self._measurements.clear()

        # Build an index.
        if self._auto_index:
            self._index.build([])

        return

    def _update_helper(
        self,
        query: Union[CompoundQuery, SimpleQuery] = None,
        time: Union[datetime, Callable, None] = None,
        measurement: Union[str, Callable, None] = None,
        tags: Union[Mapping, Callable, None] = None,
        fields: Union[Mapping, Callable, None] = None,
        _measurement: str = None,
    ):
        """ """
        """Update all matching Points in the database with new attributes.

        Args:
            query: A query as a condition.
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.

        Returns:
            A count of updated points.

        Todo:
            Update index in a smart way.
        """
        assert self._storage.can_write

        # Return value.
        update_count = 0

        # Define the function that will perform the update
        perform_update = self._generate_updater(
            query=query,
            time=time,
            measurement=measurement,
            tags=tags,
            fields=fields,
        )

        use_index = query and self._auto_index and self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if _measurement:
                mq = MeasurementQuery() == _measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No candidates from the index.
            if not index_rst._items:
                return 0

            # Candidates and we only have to check a subset.
            if len(index_rst._items) == len(self._index):
                use_index = False

        temp_memory = []
        time_updates_performed = False

        # Update with help of index.
        if use_index:

            j = 0

            for i, item in enumerate(self._storage):
                # Not a query match, pass item through.
                if j == len(index_rst.items) or i != index_rst._items[j]:
                    temp_memory.append(item)
                    continue

                _point = self._storage._deserialize_storage_item(item)

                # Candidate, no further eval necessary, update.
                if index_rst._is_complete:
                    u, t = perform_update(_point)

                    if u:
                        update_count += 1
                        temp_memory.append(
                            self._storage._serialize_point(_point)
                        )
                    else:
                        temp_memory.append(item)

                    if t:
                        time_updates_performed = True

                    continue

                # Candidate, eval and update.
                if query(_point):
                    u, t = perform_update(_point)

                    if u:
                        update_count += 1
                        temp_memory.append(
                            self._storage._serialize_point(_point)
                        )
                    else:
                        temp_memory.append(item)

                    if t:
                        time_updates_performed = True

                    continue

                # Candidate, eval is False.
                temp_memory.append(item)

                j += 1

        else:

            for item in self._storage:
                # Not this measurement.
                if (
                    _measurement
                    and self._storage._deserialize_measurement(item)
                    != _measurement
                ):
                    temp_memory.append(item)
                    continue

                _point = self._storage._deserialize_storage_item(item)

                # Query match.
                if not query or query(_point):
                    u, t = perform_update(_point)
                    if u:
                        update_count += 1

                    if t:
                        time_updates_performed = True

                    temp_memory.append(
                        self._storage._deserialize_storage_item(_point)
                    )
                    continue

                # Not a query match.
                temp_memory.append(item)

        # No updates performed. Delete temp memory and do not write.
        if not update_count:
            del temp_memory
            gc.collect()

            return 0

        # Reindex only if necessary. We reindex only if time updates were
        # performed or if the index was not previously intact.
        if self._auto_index and (
            time_updates_performed or not self._index.valid
        ):
            temp_memory.sort(
                key=lambda x: self._storage._deserialize_timestamp(x)
            )
            self._storage._write(temp_memory, True)

        # Write memory to storage.
        self._storage._write(temp_memory, self.index.valid)

        # If any item was updated, rebuild the index.
        if self._auto_index:
            self._build_index()

        # Clean up temp memory.
        del temp_memory
        gc.collect()

        return update_count
