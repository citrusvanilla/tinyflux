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
from email.generator import Generator
import os
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Union

from .point import Point

NOT_IMPLEMENTED_ERROR = NotImplementedError(
    "Derived class must implement this method."
)


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


class Storage(ABC):  # pragma: no cover
    """The abstract base class for all storage types for TinyFlux.

    Defines an extensible, static interface with required read/write ops and
    index-related getter/setters.

    Custom storage classes should inheret like so:
        >>> from tinyflux import Storage
        >>> class MyStorageClass(Storage):
                ...
    """

    _index_intact: bool = False

    def _index_sorter(self, points: List[Point]) -> None:
        """Sort function for an index.

        Args:
            points: Reference to a list of Points.
        """
        points.sort(key=lambda point: point.time)

    @property
    def index_intact(self) -> bool:
        """Get index intact attribute."""
        return self._index_intact

    @index_intact.setter
    def index_intact(self, value: bool) -> None:
        """Set index intact attribute."""
        self._index_intact = value

    @property
    def index_sorter(self) -> Callable:
        """Get index sorter function."""
        return self._index_sorter

    @index_sorter.setter
    def index_sorter(self, value: Callable) -> None:
        """Set index sorter function.

        The sorter must first sort by time, while secondary sorting attributes
        may be defined by the implementation.

        Example of sorting functions:
            >>> f1 = lambda pts: pts.sort(key=lambda p: p.time)
            >>> f2 = lambda pts: pts.sort(
                    key=lambda p: (p.time, p.measurement)
                )
        """
        self._index_sorter = value  # type: ignore

    @abstractmethod
    def append(self, points: List[Point]) -> None:
        """Append points to memory.

        Args:
            points: A list of Point objets.
        """
        ...

    def close(self) -> None:
        """Perform clean up ops."""
        ...

    def filter(self) -> None:
        """"""
        ...

    @abstractmethod
    def read(self, reindex_on_read: bool) -> List[Point]:
        """Read from memory.

        Re-ordering the data after a read provides TinyFlux with the ability to
        build an index.

        Args:
            reindex_on_read: Reorder memory after data is read.

        Returns:
            A list of Points.
        """
        ...

    @abstractmethod
    def write(self, points: List[Point]) -> None:
        """Write to memory.

        Args:
            points: A list of Point objects.
        """
        ...

    def search(self, func):
        """Search and evaluate storage layer row-by-row.

        Args:
            func: A function that accepts an iterator.
        """
        ...

    def filter(self, func, reindex):
        """"""
        ...

    def update(self, func, reindex):
        """ """
        ...

    def _check_for_sorted_timestamps(self):
        """"""

    def reindex(self):
        """"""
        ...

    def _deserialize_measurement(self, item):
        """"""
        ...

    def _deserialize_storage_item(self, item):
        """"""
        ...

    def _is_sorted(self):
        """"""
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
        **kwargs,
    ) -> None:
        """Init a CSVStorage instance.

        Args:
            path: Path to file.
            create_dirs: Create parent subdirectories.
            encoding: File encoding.
            access_mode: File access mode.
        """
        super().__init__()
        self._mode = access_mode
        self.kwargs = kwargs
        self._lastest_time: Optional[datetime] = None
        self._index_intact = False
        # self._index_sorter = lambda l: l.sort(key=lambda x: x.time)

        # Create the file if it doesn't exist and creating is allowed by the
        # access mode
        if any(i in self._mode for i in ("+", "w", "a")):
            create_file(path, create_dirs=create_dirs)

        # Open the file for reading/writing
        self._handle = open(path, mode=self._mode, encoding=encoding)

        # Check if there is already data in the file.
        self._check_for_existing_data()

    def __iter__(self) -> None:
        """"""
        self._handle.seek(0)

        return csv.reader(self._handle, **self.kwargs)

    def _deserialize_timestamp(self, row: List[str]) -> datetime:
        """ """
        return datetime.fromisoformat(row[self._timestamp_idx])

    def _deserialize_measurement(self, row: List[str]) -> str:
        """ """
        return row[self._measurement_idx]

    def _deserialize_storage_item(self, row: List[str]) -> Point:
        """Deserialize a row from storage to a Point."""
        return Point()._deserialize(row)

    def _serialize_point(self, point: Point) -> List[str]:
        """Serialize a point to a row for storage."""
        return point._serialize()

    def append(self, points: List[Point]) -> None:
        """Append points to the CSV store.

        Args:
            points: A list of Point objects.
        """
        csv_writer = csv.writer(self._handle, **self.kwargs)

        # Iterate over the points.
        for point in points:
            # Check for out-of-order data.
            if (
                self._index_intact
                and self._lastest_time
                and point.time < self._lastest_time
            ):
                self._index_intact = False

            # Update last time in the data store.
            self._lastest_time = point.time

            # Write the row.
            try:
                csv_writer.writerow(point._serialize())
            except Exception as e:
                raise IOError(f"Cannot write to the database: {e}")

        # Ensure the file has been written.
        self._handle.flush()
        os.fsync(self._handle.fileno())

        # Remove data that is behind the new cursor.
        self._handle.truncate()

        return

    def close(self) -> None:
        """Clean up data store.

        Closes the file.
        """
        self._handle.close()

        return

    def filter(self, func: Callable, reindex=False):
        """
        todo:
            smarter intact index?
        """
        if not any(i in self._mode for i in ("+", "w", "a")):
            raise IOError(
                f'Cannot update the database. Access mode is "{self._mode}"'
            )

        # Move the cursor to the front again.
        self._handle.seek(0)

        r = csv.reader(self._handle, **self.kwargs)

        tmp_memory = []

        # Iterate and execute function.
        items_filtered: bool = func(
            r,
            tmp_memory,
            self._serialize_point,
            self._deserialize_storage_item,
            self._deserialize_timestamp,
            self._deserialize_measurement,
        )

        # No items were removed.
        if not items_filtered:
            return

        # Delete all contents from the file..
        self._handle.seek(0)
        self._handle.truncate()

        # No more items left.
        if not tmp_memory:
            self._lastest_time = None
            self._index_intact = True
            return

        # Reindex if necessary.
        if reindex and not self._index_intact:
            tmp_memory.sort(key=lambda x: self._deserialize_timestamp(x))
            self._index_intact = True

        # Update latest timestamp.
        if self._index_intact:
            self._lastest_time = self._deserialize_timestamp(tmp_memory[-1])
        else:
            self._latest_time = None

        # Write.
        w = csv.writer(self._handle, **self.kwargs)

        for row in tmp_memory:
            w.writerow(row)

        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.truncate()

        return

    def read(self, reindex_on_read: bool) -> List[Point]:
        """Read and parse data from the store.

        If reindex_on_read is True, the data will be ordered according to the
        index_sorted function and writen to the CSV.

        Args:
            reindex_on_read: Data will be ordered and re-written.

        Returns:
            A list of Point objects.

        Todo:
            may not be necessary to deserialize points on read.
        """
        # We're reading all data, start w/ an intact index & no latest time.
        self._index_intact = True
        self._lastest_time = None

        # Get the file size by moving the cursor to the file end and reading.
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        # Return nothing for no file size.
        if not size:
            return []

        # Otherwise, move the cursor to the front again.
        self._handle.seek(0)

        r = csv.reader(self._handle, **self.kwargs)

        points = []

        # Iterate over all rows.
        for row in r:
            # Deserialize the Point.
            point = Point()._deserialize(row)

            # Check for out-of-order data.
            if (
                self._index_intact
                and self._lastest_time
                and point.time < self._lastest_time
            ):
                self._index_intact = False

            # Update last time in the data store.
            self._lastest_time = point.time

            points.append(point)

        # If the index is not intact and we need to reindex the data...
        if (not self._index_intact) and reindex_on_read:
            self._index_sorter(points)
            self.write(points)

        return points

    def reindex(self) -> None:
        """ """
        # Move cursor to beginning of file.
        self._handle.seek(0)

        # Init a container for temp memory.
        tmp_memory = []

        # Iterate over all rows.
        r = csv.reader(self._handle, **self.kwargs)
        for row in r:
            tmp_memory.append(row)

        # Sort.
        tmp_memory.sort(key=lambda x: self._deserialize_timestamp(x))

        # Delete all contents from the file..
        self._handle.seek(0)
        self._handle.truncate()

        # Write the serialized data to the file
        w = csv.writer(self._handle, **self.kwargs)
        for row in tmp_memory:
            w.writerow(row)

        # Ensure the file has been written.
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.truncate()

        # Update this classes attributes.
        self._lastest_time = self._deserialize_timestamp(tmp_memory[-1])
        self._index_intact = True
        del tmp_memory

        return

    def search(self, func: Callable) -> None:
        """Read and apply test evaluation as an iterator.

        Args:
            func: A function that accepts an iterator.

        Returns:
            A list of Point objects.
        """
        # Get the file size by moving the cursor to the file end and reading.
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        # Return nothing for no file size.
        if not size:
            return

        # Otherwise, move the cursor to the front again.
        self._handle.seek(0)

        r = csv.reader(self._handle, **self.kwargs)

        func(r, self._deserialize_storage_item, self._deserialize_measurement)

        return

    def update(self, func: Callable, reindex=False):
        """
        todo:
            smarter intact index?
        """
        if not any(i in self._mode for i in ("+", "w", "a")):
            raise IOError(
                f'Cannot update the database. Access mode is "{self._mode}"'
            )

        # Otherwise, move the cursor to the front again.
        self._handle.seek(0)

        r = csv.reader(self._handle, **self.kwargs)

        tmp_memory = []

        # Iterate and execute function.
        updates_performed: bool = func(
            r,
            tmp_memory,
            self._serialize_point,
            self._deserialize_storage_item,
            self._deserialize_timestamp,
            self._deserialize_measurement,
        )

        # No updates performed.  Exit.
        if not updates_performed:
            return

        # Delete all contents from the file.
        self._handle.seek(0)
        self._handle.truncate()

        # Sort if reindex is True.
        if reindex:
            tmp_memory.sort(key=lambda x: self._deserialize_timestamp(x))
            self._lastest_time = datetime.fromisoformat(tmp_memory[-1][0])
            self._index_intact = True

        # Write.
        w = csv.writer(self._handle, **self.kwargs)

        for row in tmp_memory:
            w.writerow(row)

        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.truncate()

        return

    def write(self, points: List[Point]) -> None:
        """Write Points to the CSV file.

        Checks each point to see if the index is intact.

        Write overwrites all content in the CSV. For appending, see the
        'append' method.

        Args:
            points: A list of Point objects to serialize and write.
        """
        # Exit.
        if not any(i in self._mode for i in ("+", "w", "a")):
            raise IOError(
                f'Cannot write to the database. Access mode is "{self._mode}"'
            )

        # Dump the existing contents.
        self._handle.seek(0)
        self._handle.truncate()

        # We're writing all data, start w/ an intact index & no latest time.
        self._lastest_time = None
        self._index_intact = True

        # Write the serialized data to the file
        w = csv.writer(self._handle, **self.kwargs)

        for point in points:
            # Check for out-of-order data.
            if (
                self._index_intact
                and self._lastest_time
                and point.time < self._lastest_time
            ):
                self._index_intact = False

            # Update latest time in the data store.
            self._lastest_time = point.time

            # Write the serialized point.
            w.writerow(point._serialize())

        # Ensure the file has been written.
        self._handle.flush()
        os.fsync(self._handle.fileno())

        # Remove data that is behind the new cursor in case the file has
        # gotten shorter
        self._handle.truncate()

        return

    def _check_for_existing_data(self) -> None:
        """Check the file for existing data, w/o reading data into memory."""
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        # If the file is empty, flip index_intact to True.
        if not size:
            self._index_intact = True

        return

    def _is_sorted(self) -> bool:
        """ """
        # We're reading all data, start w/ an intact index & no latest time.
        self._index_intact = True
        self._lastest_time = None

        self._handle.seek(0)
        r = csv.reader(self._handle, **self.kwargs)

        # Iterate over all rows.
        for row in r:
            # Deserialize the Point.
            timestamp = self._deserialize_timestamp(row)

            if self._lastest_time and timestamp < self._lastest_time:
                self._index_intact = False
                self._lastest_time = None
                return False

            self._lastest_time = timestamp

        return True


class MemoryStorage(Storage):
    """Define the in-memory storage instance for TinyFlux.

    Attributes:
        index_intact: Data is stored according to the index sorter.
        index_sorter: Function to sort data.

    Usage:
        >>> from tinyflux import MemoryStorage
        >>> db = TinyFlux(storage=MemoryStorage)
    """

    def __init__(self) -> None:
        """Init a MemoryStorage instance."""
        super().__init__()
        self._memory: List[Point] = []
        self._index_intact: bool = True
        self._index_sorter: Callable = lambda l: l.sort(key=lambda x: x.time)

    def __iter__(self) -> Generator:
        """ """
        for point in self._memory:
            yield point

    def _deserialize_measurement(self, x) -> None:
        """"""
        return x.measurement

    def _deserialize_storage_item(self, x) -> None:
        """"""
        return x

    def append(self, points: List[Point]) -> None:
        """Append points to the memory.

        Args:
            points: A list of Point objects.
        """
        for point in points:
            # Check for out-of-order data.
            if (
                self._index_intact
                and self._memory
                and point.time < self._memory[-1].time
            ):
                self._index_intact = False

            # Append point to memory.
            self._memory.append(point)

        return

    def filter(self, func, reindex=False):
        """ """
        tmp_memory = []

        # Iterate and filter.
        items_filtered: bool = func(
            (i for i in self._memory),
            tmp_memory,
            lambda x: x,
            lambda x: x,
            lambda x: x.time,
            lambda x: x.measurement,
        )

        # No items filtered. Return.
        if not items_filtered:
            return

        # No more items left.
        if not tmp_memory:
            self._index_intact = True
            self._lastest_time = None
            self._memory = []
            return

        # Reindex.
        if reindex and not self._index_intact:
            self._index_sorter(tmp_memory)
            self._index_intact = True

        # Update latest timestamp.
        if self._index_intact:
            self._lastest_time = tmp_memory[-1].time
        else:
            self._latest_time = None

        self._memory = tmp_memory

        return

    def read(self, reindex_on_read=False) -> List[Point]:
        """Read data from the store.

        If reindex_on_read is True, the data will be ordered according to the
        index_sorted function.

        Args:
            reindex_on_read: Data will be ordered in memory.

        Returns:
            A list of Point objects.
        """
        # Reindex if necessary.
        if (not self._index_intact) and reindex_on_read:
            self._index_sorter(self._memory)
            self._index_intact = True

        return self._memory

    def reindex(self) -> None:
        """ """
        self._index_sorter(self._memory)
        self._index_intact = True

        return

    def search(self, func: Callable) -> Union[Point, List[Point], None]:
        """Read and apply test evaluation as an iterator.

        Args:
            func: A function that accepts an iterator.

        Returns:
            A list of Point objects.
        """
        return func(
            (i for i in self._memory),
            lambda x: x,
            self._deserialize_measurement,
        )

    def update(self, func, reindex):
        """ """
        tmp_memory = []

        # Iterate and filter.
        items_updated: bool = func(
            (i for i in self._memory),
            tmp_memory,
            lambda x: x,
            lambda x: x,
            lambda x: x.time,
            lambda x: x.measurement,
        )

        # No items updated. Return.
        if not items_updated:
            return

        self._memory = tmp_memory

        if reindex and not self._index_intact:
            self._index_sorter(self._memory)
            self._index_intact = True
            self._lastest_time = (
                self._memory[-1].time if self._memory else None
            )

        return

    def write(self, points) -> None:
        """Write Points to memory.

        Checks each point to see if the index is intact.

        Write overwrites all content in memory. For appending, see the
        'append' method.

        Args:
            points: A list of Point objects to serialize and write.
        """
        self._memory = []
        self._index_intact = True

        for point in points:
            # Check for out-of-order data.
            if (
                self._index_intact
                and self._memory
                and point.time < self._memory[-1].time
            ):
                self._index_intact = False

            self._memory.append(point)

        return

    def _is_sorted(self) -> bool:
        """ """
        # We're reading all data, start w/ an intact index & no latest time.
        self._index_intact = True
        self._lastest_time = None

        # Iterate over all rows.
        for point in self._memory:
            # Deserialize the Point.
            timestamp = point.time

            if self._lastest_time and timestamp < self._lastest_time:
                self._index_intact = False
                self._lastest_time = None
                return False

            self._lastest_time = timestamp

        return True
