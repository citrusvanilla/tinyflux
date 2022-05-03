"""Tests for the tinyflux.storages module."""
import csv
from datetime import datetime, timedelta
import os
import random
import re
import tempfile
from typing import Callable

import pytest

from tinyflux import TinyFlux, Point
from tinyflux.queries import TagQuery
from tinyflux.storages import CSVStorage, MemoryStorage, Storage

random.seed()


def test_csv_write_read(tmpdir):
    """Test basic read/write to CSV."""
    # Write contents
    path = os.path.join(tmpdir, "test.csv")
    storage = CSVStorage(path)

    data = [
        Point(
            time=datetime.utcnow(),
            tags={
                "city": "los angeles",
                "neighborhood": "chinatown",
            },
            fields={"temp_f": 71.2},
        )
    ]
    storage.append(data)

    # Verify contents
    assert data == storage.read()
    storage.close()


def test_csv_kwargs(tmpdir):
    """Test kwargs propogation."""
    dbfile = os.path.join(tmpdir, "test.csv")

    # Pass the 'delimiter' kwarg from csv module.
    db = TinyFlux(str(dbfile), delimiter="|")

    # Write contents.
    p1 = Point(
        time=datetime.utcnow(),
        tags={"city": "los angeles"},
        fields={"temp_f": 71.2},
    )

    db.insert(p1)

    data = open(dbfile).read()

    assert re.fullmatch(
        r"[0-9-]{5,}T[0-9.:]{7,}"
        r"\|_default"
        r"\|_tag_city\|los angeles"
        r"\|_field_temp_f\|71.2",
        data.strip(),
    )


def test_writes_on_read_only_csv(tmpdir):
    """Test writing to a CSV opened in read-only mode."""
    dbfile = os.path.join(tmpdir, "test.csv")

    # Write to a CSV.
    db = TinyFlux(str(dbfile))
    p1 = Point(
        time=datetime.utcnow(),
        tags={"city": "los angeles"},
        fields={"temp_f": 71.2},
    )
    db.insert(p1)
    db.close()

    # Open the CSV in read-only mode.
    db = TinyFlux(str(dbfile), access_mode="r")

    assert db.get(TagQuery().city == "los angeles") == p1

    with pytest.raises(IOError):
        db.insert(Point())

    with pytest.raises(IOError):
        db.update(TagQuery().noop(), measurement="test")

    with pytest.raises(IOError):
        db.remove_all()


def test_create_dirs():
    """Test creation of directories for DB path."""
    temp_dir = tempfile.gettempdir()

    while True:
        dname = os.path.join(temp_dir, str(random.getrandbits(20)))
        if not os.path.exists(dname):
            dbdir = dname
            dbfile = os.path.join(dbdir, "db.json")
            break

    with pytest.raises(IOError):
        CSVStorage(dbfile)

    CSVStorage(dbfile, create_dirs=True).close()
    assert os.path.exists(dbfile)

    # Use create_dirs with already existing directory
    CSVStorage(dbfile, create_dirs=True).close()
    assert os.path.exists(dbfile)

    os.remove(dbfile)
    os.rmdir(dbdir)


def test_csv_invalid_directory():
    """Test invalid filepath."""
    with pytest.raises(FileNotFoundError):
        TinyFlux("/this/is/an/invalid/path/db.csv", storage=CSVStorage)


def test_in_memory_writes():
    """Test writing to MemoryStorage instances."""
    # Write contents
    storage = MemoryStorage()
    p1 = Point(
        time=datetime.utcnow(),
        tags={
            "city": "los angeles",
            "neighborhood": "chinatown",
        },
        fields={"temp_f": 71.2},
    )

    storage._write([p1])

    # Verify contents
    assert storage.read() == [p1]

    other = MemoryStorage()
    other._write([])
    assert other.read() != storage.read()


def test_in_memory_close():
    """Test close method of MemoryStorage."""
    with TinyFlux(storage=MemoryStorage) as db:
        db.insert(
            Point(
                time=datetime.utcnow(),
                tags={
                    "city": "los angeles",
                    "neighborhood": "chinatown",
                },
                fields={"temp_f": 71.2},
            )
        )

    # Invoke close.  It should not raise an error.
    db.close()


def test_subclassing_storage():
    """Test subclassing ABC Storage without defining abstract methods."""
    # Subclass without abstract methods.
    class MyStorage(Storage):
        pass

    with pytest.raises(TypeError):
        MyStorage()

    class MyStorage2(Storage):
        """Dummy storage subclass."""

        def __iter__(self):
            """Iterate."""

        def append(self, _):
            """Append method."""

        def filter(self, _):
            """Filter method."""

        def read(self, _):
            """Read method."""

        def reindex(self):
            """Reindex method."""

        def search(self):
            """Seach method."""

        def update(self, func, reindex):
            """Update method."""

        def _deserialize_measurement(self, item):
            """Deserialize measurement."""
            ...

        def _deserialize_timestamp(self, item):
            """Deserialize timestamp."""
            ...

        def _deserialize_storage_item(self, item):
            """Deserialize storage."""
            ...

        def _is_sorted(self) -> bool:
            """Check if the storage layer is sorted."""
            ...

        def _serialize_point(self, Point) -> None:
            """Serialize Point."""
            ...

        def _write(self, _):
            """Write method."""

    s = MyStorage2()

    # Class properties that are not overridden.
    assert not s.index_intact
    assert isinstance(s._index_sorter, Callable)


def test_read_and_insert_count(mem_storage_with_counters):
    """Test read/write counts after a sequence of read/write ops."""
    with TinyFlux(auto_index=True, storage=mem_storage_with_counters) as db:
        assert (
            db.storage.reindex_count == 0
            and db.storage.append_count == 0
            and db.storage.write_count == 0
        )

        # Reference to individual measurement.  No reads should be performed.
        _ = db.measurement(db.default_measurement_name)
        assert (
            db.storage.reindex_count
            == db.storage.append_count
            == db.storage.write_count
            == 0
        )

        # Get all points.
        db.all()
        assert (
            db.storage.reindex_count == 0
            and db.storage.append_count == 0
            and db.storage.write_count == 0
        )

        # Insert a point in-order
        db.insert(Point())
        assert (
            db.storage.reindex_count == 0
            and db.storage.append_count == 1
            and db.storage.write_count == 0
        )

        # Get all points.
        db.all()
        assert (
            db.storage.reindex_count == 0
            and db.storage.append_count == 1
            and db.storage.write_count == 0
        )

        # Insert a point in-order
        db.insert(Point())
        assert (
            db.storage.reindex_count == 0
            and db.storage.append_count == 2
            and db.storage.write_count == 0
        )

        # Get all points.
        db.all()
        assert (
            db.storage.reindex_count == 0
            and db.storage.append_count == 2
            and db.storage.write_count == 0
        )


def test_insert_out_of_time_order(tmpdir):
    """Test that insertions are append-only, regardless of timestamp."""
    path = os.path.join(tmpdir, "test.csv")
    db = TinyFlux(path, storage=CSVStorage)

    t_past = datetime.utcnow() - timedelta(days=10)
    t_present = datetime.utcnow()
    t_future = datetime.utcnow() + timedelta(days=10)

    p_past = Point(time=t_past)
    p_present = Point(time=t_present)
    p_future = Point(time=t_future)

    db.insert(p_future)
    db.insert(p_present)
    db.insert(p_past)

    with open(path) as f:
        r = csv.reader(f)
        csv_data = [i for i in r]

    assert Point()._deserialize_from_list(csv_data[-1]).time == t_past
    assert Point()._deserialize_from_list(csv_data[-2]).time == t_present
    assert Point()._deserialize_from_list(csv_data[-3]).time == t_future


def test_index_intact_csv(tmpdir):
    """Test index_intact property of a CSV storage."""
    path = os.path.join(tmpdir, "test.csv")
    storage = CSVStorage(path=path)

    # Storage should be intact by default as it is empty.
    assert storage._index_intact

    points_in_order = [
        Point(time=i)
        for i in (
            datetime.utcnow() - timedelta(days=10),
            datetime.utcnow(),
            datetime.utcnow() + timedelta(days=10),
        )
    ]

    # First point in, should not change.
    storage.append([points_in_order[-1]])
    assert storage._index_intact

    # Insert a point from a previous time.
    storage.append([points_in_order[-3]])
    assert not storage._index_intact

    # Insert a point from a later time (should still be unsorted).
    storage.append([points_in_order[-2]])
    assert not storage._index_intact

    # Verify contents.
    assert sorted(storage.read(), key=lambda x: x.time) == points_in_order

    # Delete contents.
    storage.reset()
    assert storage._index_intact
    storage.append(points_in_order)
    assert storage._index_intact
    storage.append(points_in_order[::-1])
    assert not storage._index_intact


def test_index_intact_memory():
    """Test index_intact property of a memory storage instance."""
    # Write contents
    storage = MemoryStorage()

    # Storage should be sorted by default.
    assert storage._index_intact

    points_in_order = [
        Point(time=i)
        for i in (
            datetime.utcnow() - timedelta(days=10),
            datetime.utcnow(),
            datetime.utcnow() + timedelta(days=10),
        )
    ]

    # First point in, should not change.
    storage.append([points_in_order[-1]])
    assert storage._index_intact

    # Insert a point from a previous time.
    storage.append([points_in_order[-3]])
    assert not storage._index_intact

    # Insert a point from a later time (should still be unsorted).
    storage.append([points_in_order[-2]])
    assert not storage._index_intact

    # Verify contents.
    assert sorted(storage.read(), key=lambda x: x.time) == points_in_order

    # Delete contents.
    storage.reset()
    assert storage._index_intact
    storage.append(points_in_order)
    assert storage._index_intact
    storage.append(points_in_order[::-1])
    assert not storage._index_intact


def test_connect_to_existing_csv(tmpdir, csv_storage_with_counters):
    """Test read/write counts for connecting to existing db with data."""
    # Some mock points.
    p1 = Point(time=datetime.utcnow() - timedelta(days=10))
    p2 = Point(time=datetime.utcnow())
    p3 = Point(time=datetime.utcnow() + timedelta(days=10))

    # Mock CSV store.  Insert points out of order.
    path = os.path.join(tmpdir, "test.csv")
    with open(path, "w") as f:
        w = csv.writer(f)
        w.writerow(p2._serialize_to_list())
        w.writerow(p1._serialize_to_list())

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)
    assert not storage.index_intact
    assert storage.reindex_count == 0

    # Append a point.  No reads should be performed.
    storage.append([p3])
    assert not storage.index_intact
    assert storage.reindex_count == 0

    # Read without reindexing.
    assert storage.read() == [p2, p1, p3]
    assert storage.reindex_count == 0
    assert storage.write_count == 0
    assert not storage.index_intact

    # Read with reindexing.
    pts = [p1, p2, p3]
    storage.reindex()
    assert storage.read() == pts
    assert storage.reindex_count == 1
    assert storage.write_count == 1
    assert storage.index_intact

    storage.close()

    # Connect to empty CSV.
    path = os.path.join(tmpdir, "empty_test.csv")
    empty_storage = csv_storage_with_counters(path)
    assert empty_storage.index_intact
    assert empty_storage.reindex_count == 0


def test_reindex_on_read_csv(tmpdir, csv_storage_with_counters):
    """Test multiple reads and the read/write/append counts for csv."""
    path = os.path.join(tmpdir, "test.csv")
    storage = storage = csv_storage_with_counters(path)

    # Storage should be intact by default as it is empty.
    assert storage.index_intact

    # Some mock points.
    p1 = Point(time=datetime.utcnow() - timedelta(days=10))
    p2 = Point(time=datetime.utcnow())
    p3 = Point(time=datetime.utcnow() + timedelta(days=10))

    # First point in, should not change.
    storage.append([p2])
    assert storage.index_intact
    assert storage.append_count == 1
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Insert a point from a previous time.
    storage.append([p1])
    assert not storage.index_intact
    assert storage.append_count == 2
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Insert a point from a previous time.
    storage.append([p3])
    assert not storage.index_intact
    assert storage.append_count == 3
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Read contents with reindexing.
    storage.reindex()
    assert storage.index_intact
    assert storage.append_count == 3
    assert storage.reindex_count == 1
    assert storage.write_count == 1

    # Read contents again.  Nothing should be written.
    storage.read()
    assert storage.index_intact
    assert storage.append_count == 3
    assert storage.reindex_count == 1
    assert storage.write_count == 1


def test_reindex_on_read_memory(mem_storage_with_counters):
    """Test multiple reads and the read/write/append counts for memory."""
    storage = storage = mem_storage_with_counters()

    # Storage should be intact by default as it is empty.
    assert storage.index_intact

    # Some mock points.
    p1 = Point(time=datetime.utcnow() - timedelta(days=10))
    p2 = Point(time=datetime.utcnow())
    p3 = Point(time=datetime.utcnow() + timedelta(days=10))

    # First point in, should not change.
    storage.append([p2])
    assert storage.index_intact
    assert storage.append_count == 1
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Insert a point from a previous time.
    storage.append([p1])
    assert not storage.index_intact
    assert storage.append_count == 2
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Insert a point from a previous time.
    storage.append([p3])
    assert not storage.index_intact
    assert storage.append_count == 3
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Read contents with reindexing.
    storage.read()
    assert not storage.index_intact
    assert storage.append_count == 3
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Reindex. One write should be performed.
    storage.reindex()
    assert storage.index_intact
    assert storage.append_count == 3
    assert storage.reindex_count == 1
    assert storage.write_count == 0


def test_multiple_appends(
    tmpdir, csv_storage_with_counters, mem_storage_with_counters
):
    """Test read/write counts for multiple appends in a row."""
    # Mock CSV empty. Insert points out of order.
    path = os.path.join(tmpdir, "test.csv")

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)
    assert storage.index_intact
    assert storage.reindex_count == 0

    # Append a bunch of points in-order. No reads should be performed.
    t = datetime.utcnow()

    for i in range(10):
        storage.append([Point(time=t)])
        assert storage.append_count == i + 1
        assert storage.reindex_count == 0
        assert storage.write_count == 0
        assert storage.index_intact

    storage.close()

    # Some mock points.
    p1 = Point(time=datetime.utcnow() - timedelta(days=10))
    p2 = Point(time=datetime.utcnow())

    # Mock CSV store.  Insert points out of order.
    path = os.path.join(tmpdir, "test.csv")
    with open(path, "w") as f:
        w = csv.writer(f)
        w.writerow(p2._serialize_to_list())
        w.writerow(p1._serialize_to_list())

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)

    # Append a bunch of points in-order. No reads should be performed.
    for i in range(10):
        storage.append([Point(time=t)])
        assert storage.append_count == i + 1
        assert storage.reindex_count == 0
        assert storage.write_count == 0
        assert not storage.index_intact

    storage.close()

    # Memory storage.
    storage = mem_storage_with_counters()

    # Append a bunch of points in-order. No reads should be performed.
    for i in range(10):
        storage.append([Point(time=t)])
        assert storage.append_count == i + 1
        assert storage.reindex_count == 0
        assert storage.write_count == 0
        assert storage.index_intact


def test_multiple_reads(
    tmpdir, csv_storage_with_counters, mem_storage_with_counters
):
    """Test read/write counts for multiple reads in a row."""
    # Mock CSV store.  Insert points in order.
    path = os.path.join(tmpdir, "test.csv")
    t = datetime.utcnow()

    with open(path, "w") as f:
        w = csv.writer(f)
        for _ in range(10):
            w.writerow(Point(time=t)._serialize_to_list())

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)
    assert not storage.index_intact

    # Append a bunch of points in-order. No reads should be performed.
    for i in range(5):
        storage.read()
        assert storage.reindex_count == 0
        assert storage.append_count == 0
        assert storage.write_count == 0
        assert not storage.index_intact

    storage.close()

    # Mock CSV store.  Insert points out-of-order.
    path = os.path.join(tmpdir, "test.csv")
    with open(path, "w") as f:
        w = csv.writer(f)
        for _ in range(10):
            w.writerow(Point(time=t)._serialize_to_list())
        w.writerow(
            Point(
                time=datetime.utcnow() - timedelta(days=365)
            )._serialize_to_list()
        )

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)
    assert not storage.index_intact

    # Append a bunch of points in-order. No reads should be performed.
    for i in range(5):
        storage.read()
        assert storage.reindex_count == 0
        assert storage.append_count == 0
        assert storage.write_count == 0
        assert not storage.index_intact

    storage.close()

    # Memory storage.
    storage = mem_storage_with_counters()

    # Append a bunch of points in-order. No reads should be performed.
    for i in range(10):
        storage.append([Point(time=t)])

    for i in range(5):
        storage.read()
        assert storage.append_count == 10
        assert storage.reindex_count == 0
        assert storage.write_count == 0
        assert storage.index_intact


def test_read_on_empty_file(tmpdir, csv_storage_with_counters):
    """Test read method on empty file."""
    path = os.path.join(tmpdir, "test.csv")

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)

    # Read and assert empty list.
    assert storage.read() == []
    assert storage.read() == []
