"""The main module of the TinyFlux package, containing the TinyFlux class."""
import copy
from datetime import datetime, timezone
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from .index import Index
from .measurement import Measurement
from .point import FieldValue, Point, validate_fields, validate_tags
from .queries import (
    CompoundQuery,
    MeasurementQuery,
    SimpleQuery,
    TagQuery,
    Query,
)
from .storages import CSVStorage, Storage


def append_op(method):
    """Decorate an append operation with assertion.

    Ensures storage can be appended to before doing anything.
    """

    @wraps(method)
    def op(self, *args, **kwargs):
        """Decorate."""
        assert self._storage.can_append
        return method(self, *args, **kwargs)

    return op


def read_op(method):
    """Decorate a read operation with assertion.

    Ensures storage can be read from before doing anything.
    """

    @wraps(method)
    def op(self, *args, **kwargs):
        """Decorate."""
        assert self._storage.can_read

        if self._auto_index and not self._index.valid:
            self.reindex()

        return method(self, *args, **kwargs)

    return op


def temp_storage_op(method):
    """Decorate a db operation that requires auxiliary storage.

    Initializes temporary storage, invokes method, and cleans-up storage after
    op has run.
    """

    @wraps(method)
    def op(self, *args, **kwargs):
        """Decorate."""
        # Init temp storage in the storage class.
        self._storage._init_temp_storage()

        # Invoke op.
        rst = method(self, *args, **kwargs)

        # Clean-up temp storage.
        self._storage._cleanup_temp_storage()

        return rst

    return op


def write_op(method):
    """Decorate a write operation with assertion.

    Ensures storage can be written to before doing anything.
    """

    @wraps(method)
    def op(self, *args, **kwargs):
        """Decorate."""
        assert self._storage.can_write
        return method(self, *args, **kwargs)

    return op


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

    _auto_index: bool
    _storage: Storage
    _index: Index
    _measurements: Dict[str, Measurement]
    _open: bool

    def __init__(self, *args, auto_index: bool = True, **kwargs):
        """Initialize a new instance of TinyFlux.

        If 'auto_index' is set to True, TinyFlux will check the storage layer
        for sortedness, and re-sort if necessary. An index will then be built
        in-memory for efficient querying.

        Please note, this operation can take some time.  If you need to insert
        into TinyFlux immediately after initializing the DB, set
        'auto-index' to False.

        Args:
            auto_index: Reindexing of data will be performed automatically.
            storage: Class of Storage instance.
        """
        self._auto_index = auto_index

        # Init storage.
        storage = kwargs.pop("storage", self.default_storage_class)
        self._storage = storage(*args, **kwargs)

        # Init index.
        if not isinstance(self._auto_index, bool):
            raise TypeError("'auto_index' must be True/False.")
        self._index = Index(valid=self._storage._initially_empty)

        # Init references to measurements.
        self._measurements = {}
        self._open = True

        # Reindex if auto_index is True.
        if self._auto_index and not self._storage._initially_empty:
            self.reindex()

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

    def __iter__(self) -> Iterator[Point]:
        """Return an iterater for all Points in the storage layer."""
        for item in self._storage:
            yield self._storage._deserialize_storage_item(item)

    def __len__(self):
        """Get the number of Points in the storage layer."""
        # If the index is valid, check it.
        if self._auto_index and self._index.valid:
            return len(self._index)

        # Otherwise, we get it from storage class.
        return len(self._storage)

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

    @read_op
    def all(self, sorted: bool = True) -> List[Point]:
        """Get all data in the storage layer as Points.

        Args:
            sorted: Whether or not to return points sorted by time.

        Returns:
            A list of Points.
        """
        points = self._storage.read()

        if sorted:
            points.sort(key=lambda x: x.time)

        return points

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

    @read_op
    def contains(
        self, query: Query, measurement: Optional[str] = None
    ) -> bool:
        """Check whether the database contains a point matching a query.

        Defines a function that iterates over storage items and submits it to
        the storage layer.

        Args:
            query: A Query.
            measurement: An optional measurement to filter by.

        Returns:
            True if point found, else False.
        """
        # If the index is valid, check it.
        if self._index.valid:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # Return whether or not items were found in the index.
            return len(index_rst._items) > 0

        # Return value.
        contains = False

        # Search without help of the index.
        for item in self._storage:

            # Filter by measurement.
            if (
                measurement
                and self._storage._deserialize_measurement(item) != measurement
            ):
                continue

            # Evaluate query against storage item.
            if query(self._storage._deserialize_storage_item(item)):
                contains = True
                break

        return contains

    @read_op
    def count(self, query: Query, measurement: Optional[str] = None) -> int:
        """Count the points matching a query in the database.

        Args:
            query: a Query.
            measurement: An optional measurement to filter by.

        Returns:
            A count of matching points in the measurement.
        """
        # If the index is valid, check it.
        if self._index.valid:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # Return count of items.
            return len(index_rst._items)

        # Return value.
        count = 0

        # Search without help of the index.
        for item in self._storage:

            # Filter by measurement.
            if (
                measurement
                and not self._storage._deserialize_measurement(item)
                == measurement
            ):
                continue

            if query(self._storage._deserialize_storage_item(item)):
                count += 1

        return count

    @read_op
    @write_op
    @temp_storage_op
    def drop_measurement(self, name: str) -> int:
        """Drop a specific measurement from the database.

        If 'auto-index' is True, the storage layer will be sorted after
        this function is run, and a new index will be built.

        Args:
            name: The name of the measurement.

        Returns:
            The count of removed items.

        Raises:
            OSError if storage cannot be written to.
        """
        if name in self._measurements:
            del self._measurements[name]

        return self._remove_helper(MeasurementQuery() == name, name)

    @read_op
    def get(
        self, query: Query, measurement: Optional[str] = None
    ) -> Optional[Point]:
        """Get exactly one point specified by a query from the database.

        Returns None if the point doesn't exist.

        Args:
            query: A Query.
            measurement: An optional measurement to filter by.

        Returns:
            First found Point or None.
        """
        use_index = self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No items from the index.
            if not index_rst._items:
                return None

            # Items, but it's all of them.
            if len(index_rst._items) == len(self._index):
                use_index = False

        # Return value.
        got_point = None

        # Search with help of the index.
        if use_index:

            for i, item in enumerate(self._storage):

                # Not a candidate.
                if i not in index_rst._items:
                    continue

                # Candidate, no further evaluation necessary.
                got_point = self._storage._deserialize_storage_item(item)
                break

        else:

            # Evaluate all points until match.
            for item in self._storage:

                # Filter by measurement.
                if (
                    measurement
                    and self._storage._deserialize_measurement(item)
                    != measurement
                ):
                    continue

                # Evaluate query against storage item.
                _point = self._storage._deserialize_storage_item(item)
                if query(_point):
                    got_point = _point
                    break

        # Put a timezone on it.
        if got_point:
            got_point.time.replace(tzinfo=timezone.utc)

        return got_point

    @read_op
    def get_field_keys(self, measurement: Optional[str] = None) -> List[str]:
        """Get all field keys in the database.

        Args:
            measurement: Optional measurement to filter by.

        Returns:
            List of field keys, sorted.
        """
        # If index is valid, get keys from index.
        if self._index.valid:
            return sorted(self._index.get_field_keys(measurement))

        # Otherwise, go through storage.
        rst = set({})

        for item in self._storage:

            # Filter by measurement.
            if (
                measurement
                and self._storage._deserialize_measurement(item) != measurement
            ):
                continue

            # Match, add to results.
            _point = self._storage._deserialize_storage_item(item)

            for fk in _point.fields.keys():
                rst.add(fk)

        return sorted(rst)

    @read_op
    def get_field_values(
        self, field_key: str, measurement: Optional[str] = None
    ) -> List[FieldValue]:
        """Get field values in the database.

        Args:
            field_key: Field key to get values for.
            measurement: Optional measurement to filter by.

        Returns:
            List of field values.
        """
        # If index is valid, get keys from index.
        if self._index.valid:
            return self._index.get_field_values(field_key, measurement)

        # Otherwise, go through storage.
        rst = []

        for item in self._storage:

            # Filter by measurement.
            if (
                measurement
                and self._storage._deserialize_measurement(item) != measurement
            ):
                continue

            # Match, add to results.
            _point = self._storage._deserialize_storage_item(item)

            # Filter for matching field key.
            for fk, fv in _point.fields.items():
                if fk == field_key:
                    rst.append(fv)

        return rst

    @read_op
    def get_measurements(self) -> List[str]:
        """Get the names of all measurements in the database.

        Returns:
            Names of all measurements in storage as a set.
        """
        # Check the index.
        if self._index.valid:
            return sorted(self._index.get_measurements())

        # Return value.
        names = set({})

        # Otherwise, check storage.
        for item in self._storage:
            names.add(self._storage._deserialize_measurement(item))

        return sorted(names)

    @read_op
    def get_tag_keys(self, measurement: Optional[str] = None) -> List[str]:
        """Get all tag keys in the database.

        Args:
            measurement: Optional measurement to filter by.

        Returns:
            List of field keys, sorted.
        """
        # If index is valid, get tag keys.
        if self._index.valid:
            return sorted(self._index.get_tag_keys(measurement))

        # Otherwise, go through storage.
        rst = set({})

        for item in self._storage:

            # Filter by measurement.
            if (
                measurement
                and self._storage._deserialize_measurement(item) != measurement
            ):
                continue

            # Match, add to results.
            _point = self._storage._deserialize_storage_item(item)

            for tk in _point.tags.keys():
                rst.add(tk)

        return sorted(rst)

    @read_op
    def get_tag_values(
        self,
        tag_keys: List[str] = [],
        measurement: Optional[str] = None,
    ) -> Dict[str, List[str]]:
        """Get all tag values in the database.

        Args:
            tag_keys: Optional list of tag keys to get associated values for.
            measurement: Optional measurement to filter by.

        Returns:
            Mapping of tag_keys to associated tag values as a sorted list.
        """
        # If index is valid, get tag values.
        if self._index.valid:
            rst = self._index.get_tag_values(tag_keys, measurement)
            return {i: sorted(j) for i, j in rst.items()}

        # Otherwise, go through storage.
        relevant_tags = set(tag_keys)
        rst = {i: set({}) for i in sorted(relevant_tags)}

        for item in self._storage:

            # Filter by measurement.
            if (
                measurement
                and self._storage._deserialize_measurement(item) != measurement
            ):
                continue

            # Match, add to results.
            _point = self._storage._deserialize_storage_item(item)

            for tk, tv in _point.tags.items():
                if relevant_tags and tk not in relevant_tags:
                    continue

                rst[tk] = rst[tk].union({tv}) if tk in rst else set([tv])

        return {i: sorted(j) for i, j in rst.items()}

    @read_op
    def get_timestamps(
        self, measurement: Optional[str] = None
    ) -> List[datetime]:
        """Get all timestamps in the database.

        Returns timestamps in order of insertion in the database, as time-aware
        datetime objects with UTC timezone.

        Args:
            measurement: Optional measurement to filter by.

        Returns:
            List of timestamps by insertion order.
        """
        # If index is valid, get timestamps.
        if self._index.valid:
            return [
                datetime.fromtimestamp(i).astimezone(timezone.utc)
                for i in self._index.get_timestamps(measurement)
            ]

        # Otherwise, go through storage.
        rst: List[datetime] = []

        for item in self._storage:

            # Filter by measurement.
            if (
                measurement
                and self._storage._deserialize_measurement(item) != measurement
            ):
                continue

            # Match, add to results.
            _time = self._storage._deserialize_timestamp(item)

            rst.append(_time.replace(tzinfo=timezone.utc))

        return rst

    @append_op
    def insert(self, point: Point, measurement: Optional[str] = None) -> int:
        """Insert a Point into the database.

        Args:
            point: A Point object.
            measurement: An optional measurement to filter by.

        Returns:
            1 if success.

        Raises:
            OSError if storage cannot be appendex to.
            TypeError if point is not a Point instance.
        """
        return self._insert_helper([point], measurement)

    @append_op
    def insert_multiple(
        self, points: Iterable[Any], measurement: Optional[str] = None
    ) -> int:
        """Insert Points into the database.

        Args:
            points: An iterable of Point objects.
            measurement: An optional measurement to insert Points into.

        Returns:
            The count of inserted points.

        Raises:
            OSError if storage cannot be appendex to.
            TypeError if point is not a Point instance.
        """
        return self._insert_helper(points, measurement)

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

    def reindex(self) -> None:
        """Build a new in-memory index.

        Raises:
            OSError if storage cannot be written to.
        """
        assert self._storage.can_read

        # Pass if the index is already valid.
        if self._index.valid:
            print("Index already valid.")
            return

        # Build the index.
        self._index.build(
            self._storage._deserialize_storage_item(i) for i in self._storage
        )

        return

    @read_op
    @write_op
    @temp_storage_op
    def remove(self, query: Query, measurement: Optional[str] = None) -> int:
        """Remove Points from this database by query.

        This is irreversible.

        Args:
            query: A query to remove Points by.
            measurement: An optional measurement to filter by.

        Returns:
            The count of removed points.

        Raises:
            OSError if storage cannot be written to.
        """
        return self._remove_helper(query, measurement)

    @write_op
    def remove_all(self) -> None:
        """Remove all Points from this database.

        This is irreversible.

        Raises:
            OSError if storage cannot be written to.
        """
        self._reset_database()

        return

    @read_op
    def search(
        self,
        query: Query,
        measurement: Optional[str] = None,
        sorted: bool = True,
    ) -> List[Point]:
        """Get all points specified by a query.

        Args:
            query: A Query.
            measurement: An optional measurement to filter by.
            sorted: Whether or not to return the points sorted by time.

        Returns:
            A list of found Points.
        """
        use_index = self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No items from the index.
            if not index_rst._items:
                return []

            # Items, but it's all of them.
            if len(index_rst._items) == len(self._index):
                use_index = False

        # Return value.
        found_points: List[Point] = []

        # Search using help of index.
        if use_index:
            j = 0

            for i, item in enumerate(self._storage):

                # Not a candidate, skip.
                if i not in index_rst._items:
                    continue

                # Match or candidate match.
                found_points.append(
                    self._storage._deserialize_storage_item(item)
                )

                j += 1

                # If we are out of items, break.
                if j == len(index_rst._items):
                    break

        # Search without index.
        else:

            for item in self._storage:

                # Filter by measurement.
                if (
                    measurement
                    and self._storage._deserialize_measurement(item)
                    != measurement
                ):
                    continue

                # Match, add to results.
                _point = self._storage._deserialize_storage_item(item)
                if query(_point):
                    found_points.append(_point)

        # Put a timezone on it.
        for fp in found_points:
            fp.time.replace(tzinfo=timezone.utc)

        # Sort.
        if sorted:
            found_points.sort(key=lambda x: x.time)

        return found_points

    @read_op
    def select(
        self,
        select_keys: Union[str, Iterable[str]],
        query: Query,
        measurement: Optional[str] = None,
    ) -> List[Union[Any, Tuple[Any, ...]]]:
        """Get specified attributes from Points specified by a query.

        'select_keys' should be an iterable of attributres including 'time',
        'measurement', and tag keys and tag values.  Passing 'tags' or 'fields'
        in the 'select_keys' iterable will not retrieve all tag and/or field
        values.  Tag and field keys must be specified individually.

        Args:
            select_keys: A Point attribute or iterable of Point attributes.
            query: A Query.
            measurement: An optional measurement to filter by.

        Returns:
            A list of Point attribute values.
        """
        # Validate bad keys.
        if not hasattr(select_keys, "__iter__"):
            raise ValueError("'keys' must be a string or iterable of strings.")

        keys: List[Any] = (
            [select_keys]
            if isinstance(select_keys, str)
            else list(select_keys)
        )

        # Validate bad keys.
        for key in keys:
            if key == "time":
                continue
            elif key == "measurement":
                continue
            elif key.startswith("tags.") and len(key) > 5:
                continue
            elif key.startswith("fields.") and len(key) > 7:
                continue
            else:
                raise ValueError(f"Invalid key `{key}`.")

        use_index = self._index.valid

        results = []

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No items from the index.
            if not index_rst._items:
                return []

            j = 0

            for i, item in enumerate(self._storage):

                # Not in result set, skip.
                if i not in index_rst._items:
                    continue

                _point = self._storage._deserialize_storage_item(item)

                # Result set.
                result = []

                for key in keys:
                    if key == "time":
                        result.append(_point.time)
                    elif key == "measurement":
                        result.append(_point.measurement)
                    elif key.startswith("tags."):
                        tag_key = key[5:]

                        if (
                            tag_key in self._index._tags
                            and tag_key in _point.tags
                        ):
                            result.append(_point.tags[tag_key])
                        else:
                            result.append(None)
                    else:  # key.startswith("fields."):
                        field_key = key[7:]

                        if (
                            field_key in self._index._fields
                            and field_key in _point.fields
                        ):
                            result.append(_point.fields[field_key])
                        else:
                            result.append(None)

                results.append(result)

                j += 1

                # If we are out of items, break.
                if j == len(index_rst._items):
                    break

        # Select without index.
        else:

            for item in self._storage:

                # Filter by measurement.
                if (
                    measurement
                    and self._storage._deserialize_measurement(item)
                    != measurement
                ):
                    continue

                # Match, add to results.
                _point = self._storage._deserialize_storage_item(item)

                if query(_point):

                    result = []

                    for key in keys:
                        if key == "time":
                            result.append(_point.time)
                        elif key == "measurement":
                            result.append(_point.measurement)
                        elif key.startswith("tags."):
                            tag_key = key[5:]

                            if tag_key in _point.tags:
                                result.append(_point.tags[tag_key])
                            else:
                                result.append(None)
                        else:  # key.startswith("fields."):
                            field_key = key[7:]

                            if field_key in _point.fields:
                                result.append(_point.fields[field_key])
                            else:
                                result.append(None)

                    results.append(result)

        return [i[0] if len(keys) == 1 else tuple(i) for i in results]

    @read_op
    @write_op
    @temp_storage_op
    def update(
        self,
        query: Query,
        time: Union[datetime, Callable[[datetime], datetime], None] = None,
        measurement: Union[str, Callable[[str], str], None] = None,
        tags: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
        fields: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
        _measurement: Optional[str] = None,
    ) -> int:
        """Update all matching Points in the database with new attributes.

        Args:
            query: A query as a condition.
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.
            _measurement: An optional Measurement to filter by.

        Returns:
            A count of updated points.

        Raises:
            OSError if storage cannot be written to.
        """
        return self._update_helper(
            False, query, time, measurement, tags, fields, _measurement
        )

    @read_op
    @write_op
    @temp_storage_op
    def update_all(
        self,
        time: Union[datetime, Callable[[datetime], datetime], None] = None,
        measurement: Union[str, Callable[[str], str], None] = None,
        tags: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
        fields: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
    ) -> int:
        """Update all points in the database with new attributes.

        Args:
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.

        Returns:
            A count of updated points.

        Raises:
            OSError if storage cannot be written to.
        """
        return self._update_helper(
            True, TagQuery().noop(), time, measurement, tags, fields, None
        )

    def _generate_updater(
        self,
        query: Query,
        time: Union[datetime, Callable[[datetime], datetime], None] = None,
        measurement: Union[str, Callable[[str], str], None] = None,
        tags: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
        fields: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
    ) -> Callable[[Point], bool]:
        """Generate a routine that updates a Point with new attributes.

        Performs validation of attribute arguments.

        The update function will also validate after update. This makes this
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
        # Assert arguments are valid.
        if query and not isinstance(query, (SimpleQuery, CompoundQuery)):
            raise ValueError("Argument 'query' must be a TinyFlux Query.")

        if not (time or measurement or tags or fields):
            raise ValueError(
                "Must include time, measurement, tags, and/or fields."
            )

        # Validate non-callable update arguments.
        if time and not callable(time) and not isinstance(time, datetime):
            raise ValueError("Time must be datetime object.")

        if (
            measurement
            and not callable(measurement)
            and not isinstance(measurement, str)
        ):
            raise ValueError("Measurement must be a string.")

        if tags and not callable(tags):
            validate_tags(tags)

        if fields and not callable(fields):
            validate_fields(fields)

        # Define the update function.
        def perform_update(point: Point) -> bool:
            """Update points.

            Args:
                point: A Point.

            Returns:
                {bool} Whether a Point was actually updated.
            """
            old_point = copy.deepcopy(point)

            if time:
                if callable(time):
                    try:
                        point.time = time(point.time)
                    except ValueError:
                        raise ValueError(
                            "Time must update to a datetime object."
                        )
                else:
                    point.time = time

            if measurement:
                if callable(measurement):
                    try:
                        point.measurement = measurement(point.measurement)
                    except ValueError:
                        raise ValueError(
                            "Measurement must update to a string."
                        )
                else:
                    point.measurement = measurement

            if tags:
                if callable(tags):
                    try:
                        point.tags.update(tags(point.tags))
                    except ValueError:
                        raise ValueError("Tags must update to a valid TagSet.")

                else:
                    point.tags.update(tags)

            if fields:
                if callable(fields):
                    try:
                        point.fields.update(fields(point.fields))
                    except ValueError:
                        raise ValueError(
                            "Fields must update to a valid FieldSet."
                        )
                else:
                    point.fields.update(fields)

            return point != old_point

        return perform_update

    def _insert_helper(
        self, points: Iterable[Any], measurement: Optional[str]
    ) -> int:
        """Insert point helper.

        Args:
            updater: Update function.
            measurement: Optional measurement to insert into.

        Returns:
            Count of number of updates made.
        """
        t = datetime.now(timezone.utc)
        count = 0

        for point in points:
            if not isinstance(point, Point):
                raise TypeError("Data must be a Point instance.")

            # Update the measurement name if it doesn't match.
            if measurement and point.measurement != measurement:
                point.measurement = measurement

            # Add time if not exists.
            if point.time:
                point.time = point.time.astimezone(timezone.utc)
            else:
                point.time = t

            # Insert the points into storage.
            self._storage.append([self._storage._serialize_point(point)])

            # Check index.
            if self._auto_index and self._index.valid:
                if (
                    not self._index.empty
                    and point.time < self._index.lateset_time
                ):
                    self._index.invalidate()
                else:
                    self._index.insert([point])

            count += 1

        # Invalidate index.
        if count and not self._auto_index and self._index.valid:
            self._index.invalidate()

        return count

    def _remove_helper(
        self, query: Query, measurement: Optional[str] = None
    ) -> int:
        """Remove Points from this database by query.

        This is irreversible.

        Args:
            query: A query to remove Points by.
            measurement: An optional measurement to filter by.

        Returns:
            The count of removed points.

        Raises:
            OSError if storage cannot be written to.
        """
        use_index = self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if measurement:
                mq = MeasurementQuery() == measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No items from the index found for removal.
            if not index_rst._items:
                return 0

            # Items, but it's all of them.
            if len(index_rst.items) == len(self._index):
                self._reset_database()
                return len(index_rst.items)

        # A set of items marked for removal.
        removed_items = set({})

        # A mapping of items' old positions to new ones after update.
        updated_items: Dict[int, int] = {}

        # A counter to keep track of a remaining item's position in storage.
        new_position = 0

        # A counter of how many items were retained.
        keep_count = 0

        # Update with the help of the index.
        if use_index:

            j = 0

            for i, item in enumerate(self._storage):

                # No more items or item is not a candidate.
                if j == len(index_rst._items) or i not in index_rst._items:
                    self._storage.append([item], temporary=True)

                    # Add to updated_items if the item has a new position.
                    if i != new_position:
                        updated_items[i] = new_position

                    new_position += 1
                    keep_count += 1
                    continue

                # Candidate and no further evaluation necessary.
                removed_items.add(i)

                j += 1

        # Update without the help of the index.
        else:

            for i, item in enumerate(self._storage):

                # Filter by measurement.
                if measurement:
                    _measurement = self._storage._deserialize_measurement(item)

                    # Not this measurement, keep.
                    if _measurement != measurement:
                        self._storage.append([item], temporary=True)
                        keep_count += 1
                        continue

                # Match, filter.
                if query(self._storage._deserialize_storage_item(item)):
                    removed_items.add(i)

                # Not a match, keep.
                else:
                    self._storage.append([item], temporary=True)
                    keep_count += 1

        # No items removed, delete temporary memory and do not update storage.
        if not len(removed_items):
            return 0

        # No items remaining. Clear out storage, clear out index.
        if not keep_count:
            self._reset_database()
            return len(removed_items)

        # Items were updated. Swap storages and clean up.
        self._storage._swap_temp_with_primary()

        # We are auto_indexing, update index. Otherwise, invalidate it.
        if self._auto_index:
            self._index.remove(removed_items)
            self._index.update(updated_items)
        else:
            self._index.invalidate()

        # Return number of updated items.
        return len(removed_items)

    def _reset_database(self) -> None:
        """Reset TinyFlux and storage."""
        # Write empty list to storage.
        self._storage.reset()

        # Drop measurements.
        self._measurements.clear()

        # Build an index.
        if self._auto_index:
            self._index._reset()
        else:
            self._index.invalidate()

        return

    def _update_helper(
        self,
        update_all: bool,
        query: Query,
        time: Union[datetime, Callable[[datetime], datetime], None] = None,
        measurement: Union[str, Callable[[str], str], None] = None,
        tags: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
        fields: Union[Mapping, Callable[[Mapping], Mapping], None] = None,
        _measurement: Optional[str] = None,
    ) -> int:
        """Update all matching Points in the database with new attributes.

        Args:
            update_all: Whether or not all Points should be updated.
            query: A query as a condition.
            time: A datetime object or Callable returning one.
            measurement: A string or Callable returning one.
            tags: A mapping or Callable returning one.
            fields: A mapping or Callable returning one.
            _measurement: Optional measurement filter.

        Returns:
            A count of updated points.
        """
        # Return value.
        update_count = 0

        # Define the function that will perform the update.
        perform_update = self._generate_updater(
            query=query,
            time=time,
            measurement=measurement,
            tags=tags,
            fields=fields,
        )

        use_index = not update_all and self._index.valid

        # If we are auto-indexing and the index is valid, check it.
        if use_index:

            if _measurement:
                mq = MeasurementQuery() == _measurement
                index_rst = self._index.search(mq & query)
            else:
                index_rst = self._index.search(query)

            # No items from the index.
            if not index_rst._items:
                return 0

            # Items and we only have to check a subset.
            if len(index_rst._items) == len(self._index):
                use_index = False

        # Update with the help of the index.
        if use_index:

            j = 0

            for i, item in enumerate(self._storage):

                # Not a query match, pass item through.
                if j == len(index_rst.items) or i not in index_rst._items:
                    self._storage.append([item], temporary=True)
                    continue

                _point = self._storage._deserialize_storage_item(item)

                # Attempt update.
                u = perform_update(_point)

                # Attributes changed. Serialize and add to memory.
                if u:
                    self._storage.append(
                        [self._storage._serialize_point(_point)],
                        temporary=True,
                    )

                    update_count += 1
                    continue

                # Attributes unchanged.
                else:
                    self._storage.append([item], temporary=True)

                j += 1

        # Update without the help of the index.
        else:

            for item in self._storage:

                # Filter by measurement.
                if (
                    _measurement
                    and self._storage._deserialize_measurement(item)
                    != _measurement
                ):
                    self._storage.append([item], temporary=True)
                    continue

                _point = self._storage._deserialize_storage_item(item)

                # No query specified, or query match.
                if update_all or query(_point):

                    # Attempt update.
                    u = perform_update(_point)

                    # Attributes changed. Serialize and add to memory.
                    if u:
                        self._storage.append(
                            [self._storage._deserialize_storage_item(_point)],
                            temporary=True,
                        )

                        update_count += 1
                        continue

                # Not a query match or attributes unchanged.
                self._storage.append([item], temporary=True)

        # No updates performed. Delete temp memory and do not write.
        if not update_count:
            return 0

        # Items were updated. Swap storages and clean up.
        self._storage._swap_temp_with_primary()

        # Invalidate index.
        self._index.invalidate()

        # If any item was updated, rebuild the in-memory index.
        if self._auto_index:
            self._index.build(
                self._storage._deserialize_storage_item(i)
                for i in self._storage
            )

        return update_count
