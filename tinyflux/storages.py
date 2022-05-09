"""Defintion of TinyFlux storages classes.

Storage defines an abstract base case using the built-in ABC of python. This
class defines the requires abstract methods of read, write, and append, as well
as getters and setters for attributes required to reindex the data.

A storage object will manage data with a file handle, or in memory.

A storage class is provided to the TinyFlux facade as an initial argument.  The
TinyFlux instance will manage the lifecycle of the storage instance.

Usage:
    >>> my_mem_db = TinyFlux(storage=MemoryStorage)
    >>> my_csv_db = TinyFlux('path/to/my.csv', storage=CSVStorage)
"""
from abc import ABC, abstractmethod
import csv
from datetime import datetime
import os
from pathlib import Path
import shutil
from tempfile import NamedTemporaryFile

from typing import (
    Any,
    Iterator,
    List,
    Sequence,
    Union,
)
from typing_extensions import TypeAlias

from .point import Point

MemStorageItem: TypeAlias = Point
CSVStorageItem: TypeAlias = Sequence[str]


def create_file(path: Union[str, Path], create_dirs: bool) -> None:
    """Create a file if it doesn't exist yet.

    Args:
        path: The file to create.
        create_dirs: Whether to create all missing parent directories.
    """
    if create_dirs:
        base_dir = os.path.dirname(path)

        # Check if we need to create missing parent directories
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

    # Create the file by opening it in 'a' mode which creates the file if it
    # does not exist yet but does not modify its contents
    with open(path, "a"):
        pass

    return


def append_op(method):
    """Decorate an append operation with assertion."""

    def op(self, *args, **kwargs):
        """Decorate."""
        assert self._storage.can_append
        return method(self, *args, **kwargs)

    return op


def read_op(method):
    """Decorate a read operation with assertion."""

    def op(self, *args, **kwargs):
        """Decorate."""
        assert self._storage.can_read

        if self._auto_index and not self._index.valid:
            self.reindex()

        return method(self, *args, **kwargs)

    return op


def write_op(method):
    """Decorate a write operation with assertion."""

    def op(self, *args, **kwargs):
        """Decorate."""
        assert self._storage.can_write
        return method(self, *args, **kwargs)

    return op


class Storage(ABC):  # pragma: no cover
    """The abstract base class for all storage types for TinyFlux.

    Defines an extensible, static interface with required read/write ops and
    index-related getter/setters.

    Custom storage classes should inheret like so:
        >>> from tinyflux import Storage
        >>> class MyStorageClass(Storage):
                ...
    """

    _initially_empty: bool

    @property
    def can_append(self) -> bool:
        """Can append to DB."""
        return True

    @property
    def can_read(self) -> bool:
        """Can read the DB."""
        return True

    @property
    def can_write(self) -> bool:
        """Can write to DB."""
        return True

    @abstractmethod
    def __iter__(self) -> Iterator:
        """Return a generator for items in storage."""
        ...

    @abstractmethod
    def append(self, points: List[Point], temporary=False) -> None:
        """Append points to the store.

        Args:
            points: A list of Point objets.
            temporary: Whether or not to append to temporary storage.
        """
        ...

    def close(self) -> None:
        """Perform clean up ops."""
        ...

    @abstractmethod
    def read(self) -> List[Point]:
        """Read from the store.

        Re-ordering the data after a read provides TinyFlux with the ability to
        build an index.

        Args:
            reindex_on_read: Reorder the store after data is read.

        Returns:
            A list of Points.
        """
        return list(self._deserialize_storage_item(i) for i in iter(self))

    def reset(self) -> None:
        """Reset the storage instance.

        Removes all data.
        """
        self._write([])
        self._write([], temporary=True)

        return

    @abstractmethod
    def _deserialize_measurement(self, item: Any) -> str:
        """Deserialize an item from storage to a measurement."""
        ...

    @abstractmethod
    def _deserialize_timestamp(self, item: Any) -> datetime:
        """Deserialize an item from storage to a timestamp."""
        ...

    @abstractmethod
    def _deserialize_storage_item(self, item: Any) -> Point:
        """Deserialize an item from storage to a Point."""
        ...

    @abstractmethod
    def _serialize_point(self, point: Point) -> Any:
        """Serialize a point to an item for storage."""
        ...

    @abstractmethod
    def _swap_temp_with_primary(self) -> None:
        """Swap primary data store with temporary data store."""
        ...

    @abstractmethod
    def _write(self, items: List[Any], temporary=False) -> None:
        """Write to the store.

        This function should overwrite the entire file.

        Args:
            points: A list of Point objects.
            temporary: Whether or not to write to temporary storage.
        """
        ...


class CSVStorage(Storage):
    """Define the default storage instance for TinyFlux, a CSV store.

    CSV provides append-only writes, which is efficient for high-frequency
    writes, common to time-series datasets.

    Attributes:
        index_intact: Data is stored according to the index sorter.
        index_sorter: Function to sort data.

    Usage:
        >>> from tinyflux import CSVStorage
        >>> db = TinyFlux("my_csv_store.csv", storage=CSVStorage)
    """

    _timestamp_idx = 0
    _measurement_idx = 1

    def __init__(
        self,
        path: Union[str, Path],
        create_dirs: bool = False,
        encoding: str = None,
        access_mode: str = "r+",
        flush_on_insert: bool = True,
        **kwargs,
    ) -> None:
        """Init a CSVStorage instance.

        This will init a file object to the specified filepath. No reads are
        performed by default, so we don't know if the data is sorted and
        therefore, the _initially_empty attribute is set to False.

        Args:
            path: Path to file.
            create_dirs: Create parent subdirectories.
            encoding: File encoding.
            access_mode: File access mode.
        """
        super().__init__()
        self._mode = access_mode
        self.kwargs = kwargs
        self._latest_time = None
        self._initially_empty = False
        self._path = path
        self._flush_on_insert = flush_on_insert

        # Create the file if it doesn't exist and creating is allowed.
        if any(i in self._mode for i in ("+", "w", "a")):
            create_file(path, create_dirs=create_dirs)

        # Open the file for reading/writing
        self._handle = open(path, mode=self._mode, encoding=encoding)

        # Open a tempfile.
        if self._mode not in ("r+", "w", "w+"):
            tempfile = NamedTemporaryFile("w+t", newline="", delete=False)
            self._tempile_name = tempfile.name
            self._temp_handle = tempfile
        else:
            self._tempfile_name = None
            self._temp_handle = None

        # Check if there is already data in the file.
        self._check_for_existing_data()

    @property
    def can_append(self) -> bool:
        """Return whether or not appends can occur."""
        if self._mode not in ("r+", "w", "w+", "a", "a+"):
            raise IOError(
                f'Cannot update the database. Access mode is "{self._mode}"'
            )

        return True

    @property
    def can_read(self) -> bool:
        """Return whether or not reads can occur."""
        if self._mode not in ("r+", "r", "w+", "a+"):
            raise IOError(
                f'Cannot update the database. Access mode is "{self._mode}"'
            )

        return True

    @property
    def can_write(self) -> bool:
        """Return whether or not writes can occur."""
        if self._mode not in ("r+", "w", "w+"):
            raise IOError(
                f'Cannot update the database. Access mode is "{self._mode}"'
            )

        return True

    def __iter__(self) -> Iterator:
        """Return a CSV reader object that can be iterated over."""
        self._handle.seek(0)

        return csv.reader(self._handle, **self.kwargs)

    def __len__(self) -> int:
        """Return the number of items."""
        self._handle.seek(0)

        return sum(1 for _ in self._handle)

    def append(self, points: List[Point], temporary=False) -> None:
        """Append points to the CSV store.

        Args:
            points: A list of Point objects.
            temporary: Whether or not to append to temporary storage.
        """
        if temporary:
            csv_writer = csv.writer(self._temp_handle, **self.kwargs)
        else:
            csv_writer = csv.writer(self._handle, **self.kwargs)

        # Iterate over the points.
        for point in points:
            # Write the row.
            csv_writer.writerow(point._serialize_to_list())

        if self._flush_on_insert:
            # Ensure the file has been written.
            self._handle.flush()
            os.fsync(self._handle.fileno())

            # Remove data that is behind the new cursor.
            self._handle.truncate()

        return

    def close(self) -> None:
        """Clean up data store.

        Closes the file object.
        """
        self._handle.close()
        if self._temp_handle:
            self._temp_handle.close()

        return

    def read(self) -> List[Point]:
        """Read all items from the storage into memory.

        Returns:
            A list of Point objects.
        """
        return super().read()

    def _check_for_existing_data(self) -> None:
        """Check the file for existing data, w/o reading data into memory."""
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        # If the file is empty, flip index_intact to True.
        if not size:
            self._initially_empty = True

        return

    def _deserialize_measurement(self, row: CSVStorageItem) -> str:
        """Deserialize measurement from a row."""
        return row[self._measurement_idx]

    def _deserialize_storage_item(self, row: CSVStorageItem) -> Point:
        """Deserialize a row from storage to a Point."""
        return Point()._deserialize_from_list(row)

    def _deserialize_timestamp(self, row: CSVStorageItem) -> datetime:
        """Deserialize timestamp from a row."""
        return datetime.fromisoformat(row[self._timestamp_idx])

    def _serialize_point(
        self, point: Point
    ) -> Sequence[Union[str, float, int]]:
        """Serialize a point to an item for storage."""
        return point._serialize_to_list()

    def _swap_temp_with_primary(self) -> None:
        """Swap primary data store with temporary data store."""
        shutil.move(self._tempile_name, self._path)
        self._write([], temporary=True)

        return

    def _write(self, items: List[CSVStorageItem], temporary=False) -> None:
        """Write Points to the CSV file.

        Checks each point to see if the index is intact.

        Write overwrites all content in the CSV. For appending, see the
        'append' method.

        Args:
            items: A list of items to write.
            temporary: Whether or not to write to temporary storage.
        """
        # Switch on temporary arg.
        handle = self._temp_handle if temporary else self._handle

        # Dump the existing contents.
        handle.seek(0)
        handle.truncate()

        if items:

            # Write the serialized data to the file
            w = csv.writer(handle, **self.kwargs)
            w.writerows(items)

            # Ensure the file has been written.
            handle.flush()
            os.fsync(handle.fileno())

            # Remove data that is behind the new cursor in case the file has
            # gotten shorter
            handle.truncate()

        return


class MemoryStorage(Storage):
    """Define the in-memory storage instance for TinyFlux.

    Memory is cleaned up along with the parent process.

    Attributes:
        index_intact: Data is stored according to the index sorter.

    Usage:
        >>> from tinyflux import MemoryStorage
        >>> db = TinyFlux(storage=MemoryStorage)
    """

    _memory: List[MemStorageItem]

    def __init__(self) -> None:
        """Init a MemoryStorage instance."""
        super().__init__()
        self._initially_empty = True
        self._memory = []
        self._temp_memory = []

    def __iter__(self) -> Iterator:
        """Return a generator to memory that can be iterated over."""
        for point in self._memory:
            yield point

    def __len__(self) -> int:
        """Return the number of items."""
        return len(self._memory)

    def append(self, points: List[Point], temporary=False) -> None:
        """Append points to the memory.

        Args:
            points: A list of Point objects.
            temporary: Whether or not to append to temporary storage.
        """
        for point in points:
            if temporary:
                self._temp_memory.append(point)
            else:
                self._memory.append(point)

        return

    def read(self) -> List[Point]:
        """Read data from the store.

        Returns:
            A list of Point objects.
        """
        return super().read()

    def _deserialize_measurement(self, item: MemStorageItem) -> str:
        """Deserialize measurement from a point."""
        return item.measurement

    def _deserialize_storage_item(self, item: MemStorageItem) -> Point:
        """Deserialize a row from memory to a Point."""
        return item

    def _deserialize_timestamp(self, item: MemStorageItem) -> datetime:
        """Deserialize timestamp from a point."""
        return item.time

    def _serialize_point(self, point: Point) -> MemStorageItem:
        """Serialize a point to an item for storage."""
        return point

    def _swap_temp_with_primary(self) -> None:
        """Swap primary data store with temporary data store."""
        self._memory = self._temp_memory
        self._write([], temporary=True)

        return

    def _write(self, items: List[MemStorageItem], temporary=False) -> None:
        """Write Points to memory.

        Checks each point to see if the index is intact.

        Write overwrites all content in memory. For appending, see the
        'append' method.

        Args:
            items: A list of Point objects to serialize and write.
            temporary: Whether or not to write to temporary storage.
        """
        if temporary:
            del self._temp_memory
            self._temp_memory = items
        else:
            del self._memory
            self._memory = items

        return
