"""Tests for the tinyflux.storages module."""
import csv
from datetime import datetime, timezone, timedelta
import os
import random
import re
import tempfile

import pytest

from tinyflux import TinyFlux, Point
from tinyflux.queries import TagQuery
from tinyflux.storages import CSVStorage, MemoryStorage, Storage

random.seed()


def test_csv_modes(tmpdir):
    """Test exceptions for read/write/append ops for CSV modes."""
    # Dummy db.
    path = os.path.join(tmpdir, "test.csv")
    db = TinyFlux(path)
    db.insert(Point())
    db.close()

    q = TagQuery().noop()
    p = Point()

    read_ops = [
        ("all", ()),
        ("contains", (q,)),
        ("count", (q,)),
        ("get", (q,)),
        ("get_field_keys", ()),
        ("get_field_values", ("",)),
        ("get_measurements", ()),
        ("get_tag_keys", ()),
        ("get_tag_values", ()),
        ("reindex", ()),
        ("search", (q,)),
    ]

    read_and_write_ops = [
        ("drop_measurement", ("_default",)),
        ("remove", (q,)),
        ("update", (q, None, "a")),
        ("update_all", (None, "a")),
    ]

    write_ops = [
        ("remove_all", ()),
    ]

    append_ops = [("insert", (p,)), ("insert_multiple", ([p],))]

    # Test read only.
    read_only_db = TinyFlux(path, auto_index=False, access_mode="r")
    assert read_only_db._storage.can_read

    for i, args in read_ops:
        read_only_db.__getattribute__(i)(*args)

    for i, args in read_and_write_ops + write_ops + append_ops:
        with pytest.raises(IOError):
            read_only_db.__getattribute__(i)(*args)

    # Test append only.
    append_only_db = TinyFlux(path, auto_index=False, access_mode="a")
    assert append_only_db._storage.can_append

    for i, args in read_ops + read_and_write_ops + write_ops:
        with pytest.raises(IOError):
            append_only_db.__getattribute__(i)(*args)

    for i, args in append_ops:
        append_only_db.__getattribute__(i)(*args)

    # Test write only.
    write_only_db = TinyFlux(path, auto_index=False, access_mode="w")
    assert write_only_db._storage.can_write

    for i, args in read_ops + read_and_write_ops:
        with pytest.raises(IOError):
            write_only_db.__getattribute__(i)(*args)

    for i, args in write_ops + append_ops:
        write_only_db.__getattribute__(i)(*args)


def test_csv_write_read(tmpdir):
    """Test basic read/write to CSV."""
    # Write contents
    path = os.path.join(tmpdir, "test.csv")
    storage = CSVStorage(path)
    p = Point(time=datetime.now(timezone.utc), measurement="m")
    storage.append([storage._serialize_point(p)])

    # Verify contents
    assert [p] == storage.read()
    storage.close()


def test_csv_kwargs(tmpdir):
    """Test kwargs propogation."""
    dbfile = os.path.join(tmpdir, "test.csv")

    # Pass the 'delimiter' kwarg from csv module.
    db = TinyFlux(str(dbfile), delimiter="|")

    # Write contents.
    p1 = Point(
        time=datetime.now(timezone.utc),
        tags={"city": "los angeles"},
        fields={"temp_f": 71.2},
    )

    db.insert(p1)

    data = open(dbfile).read()

    assert re.fullmatch(
        r"[0-9-]{10}T[0-9.:]{8,}"
        r"\|_default"
        r"\|_tag_city\|los angeles"
        r"\|_field_temp_f\|71.2",
        data.strip(),
    )


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
        time=datetime.now(timezone.utc),
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
                time=datetime.now(timezone.utc),
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

    class MyStorage2(Storage):  # pragma: no cover
        """Dummy storage subclass."""

        def __iter__(self):
            """Iterate."""

        def append(self, _):
            """Append method."""

        def read(self, _):
            """Read method."""

        def reset(self):
            """Reindex method."""

        def _deserialize_measurement(self, item):
            """Deserialize measurement."""
            ...

        def _deserialize_timestamp(self, item):
            """Deserialize timestamp."""
            ...

        def _deserialize_storage_item(self, item):
            """Deserialize storage."""
            ...

        def _serialize_point(self, Point) -> None:
            """Serialize Point."""
            ...

        def _swap_temp_with_primary(self) -> None:
            """Swap temp and primary storage."""
            ...

        def _write(self):
            """Write method."""

    # Make sure no exceptions are thrown.
    MyStorage2()


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

    t_past = datetime.now(timezone.utc) - timedelta(days=10)
    t_present = datetime.now(timezone.utc)
    t_future = datetime.now(timezone.utc) + timedelta(days=10)

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


def test_connect_to_existing_csv(tmpdir, csv_storage_with_counters):
    """Test read/write counts for connecting to existing db with data."""
    # Some mock points.
    p1 = Point(time=datetime.now(timezone.utc) - timedelta(days=10))
    p2 = Point(time=datetime.now(timezone.utc))
    p3 = Point(time=datetime.now(timezone.utc) + timedelta(days=10))

    # Mock CSV store.  Insert points out of order.
    path = os.path.join(tmpdir, "test.csv")
    with open(path, "w") as f:
        w = csv.writer(f)
        w.writerow(p2._serialize_to_list())
        w.writerow(p1._serialize_to_list())

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)
    assert storage.reindex_count == 0

    # Append a point.  No reads should be performed.
    storage.append([storage._serialize_point(p3)])
    assert storage.reindex_count == 0

    # Read without reindexing.
    assert storage.read() == [p2, p1, p3]
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    storage.close()

    # Connect to empty CSV.
    path = os.path.join(tmpdir, "empty_test.csv")
    empty_storage = csv_storage_with_counters(path)
    assert empty_storage.reindex_count == 0

    empty_storage._write([])
    assert empty_storage.write_count == 1


def test_reindex_on_read_csv(tmpdir, csv_storage_with_counters):
    """Test multiple reads and the read/write/append counts for csv."""
    path = os.path.join(tmpdir, "test.csv")
    storage = storage = csv_storage_with_counters(path)

    # Some mock points.
    p1 = Point(time=datetime.now(timezone.utc) - timedelta(days=10))
    p2 = Point(time=datetime.now(timezone.utc))
    p3 = Point(time=datetime.now(timezone.utc) + timedelta(days=10))

    # First point in, should not change.
    storage.append([storage._serialize_point(p2)])
    assert storage.append_count == 1
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Insert a point from a previous time.
    storage.append([storage._serialize_point(p1)])
    assert storage.append_count == 2
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Insert a point from a previous time.
    storage.append([storage._serialize_point(p3)])
    assert storage.append_count == 3
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Read contents again.  Nothing should be written.
    storage.read()
    assert storage.append_count == 3
    assert storage.reindex_count == 0
    assert storage.write_count == 0


def test_reindex_on_read_memory(mem_storage_with_counters):
    """Test multiple reads and the read/write/append counts for memory."""
    storage = storage = mem_storage_with_counters()

    # Some mock points.
    p1 = Point(time=datetime.now(timezone.utc) - timedelta(days=10))
    p2 = Point(time=datetime.now(timezone.utc))
    p3 = Point(time=datetime.now(timezone.utc) + timedelta(days=10))

    # First point in, should not change.
    storage.append([p2])
    assert storage.append_count == 1
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Insert a point from a previous time.
    storage.append([p1])
    assert storage.append_count == 2
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Insert a point from a previous time.
    storage.append([p3])
    assert storage.append_count == 3
    assert storage.reindex_count == 0
    assert storage.write_count == 0

    # Read contents with reindexing.
    storage.read()
    assert storage.append_count == 3
    assert storage.reindex_count == 0
    assert storage.write_count == 0


def test_multiple_appends(
    tmpdir, csv_storage_with_counters, mem_storage_with_counters
):
    """Test read/write counts for multiple appends in a row."""
    # Mock CSV empty. Insert points out of order.
    path = os.path.join(tmpdir, "test.csv")

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)
    assert storage.reindex_count == 0

    # Append a bunch of points in-order. No reads should be performed.
    t = datetime.now(timezone.utc)

    for i in range(10):
        storage.append([storage._serialize_point(Point(time=t))])
        assert storage.append_count == i + 1
        assert storage.reindex_count == 0
        assert storage.write_count == 0

    storage.close()

    # Some mock points.
    p1 = Point(time=datetime.now(timezone.utc) - timedelta(days=10))
    p2 = Point(time=datetime.now(timezone.utc))

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
        storage.append([storage._serialize_point(Point(time=t))])
        assert storage.append_count == i + 1
        assert storage.reindex_count == 0
        assert storage.write_count == 0

    storage.close()

    # Memory storage.
    storage = mem_storage_with_counters()

    # Append a bunch of points in-order. No reads should be performed.
    for i in range(10):
        storage.append([storage._serialize_point(Point(time=t))])
        assert storage.append_count == i + 1
        assert storage.reindex_count == 0
        assert storage.write_count == 0


def test_multiple_reads(
    tmpdir, csv_storage_with_counters, mem_storage_with_counters
):
    """Test read/write counts for multiple reads in a row."""
    # Mock CSV store.  Insert points in order.
    path = os.path.join(tmpdir, "test.csv")
    t = datetime.now(timezone.utc)

    with open(path, "w") as f:
        w = csv.writer(f)
        for _ in range(10):
            w.writerow(Point(time=t)._serialize_to_list())

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)

    # Append a bunch of points in-order. No reads should be performed.
    for _ in range(5):
        storage.read()
        assert storage.reindex_count == 0
        assert storage.append_count == 0
        assert storage.write_count == 0

    storage.close()

    # Mock CSV store.  Insert points out-of-order.
    path = os.path.join(tmpdir, "test.csv")
    with open(path, "w") as f:
        w = csv.writer(f)
        for _ in range(10):
            w.writerow(Point(time=t)._serialize_to_list())
        w.writerow(
            Point(
                time=datetime.now(timezone.utc) - timedelta(days=365)
            )._serialize_to_list()
        )

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)

    # Append a bunch of points in-order. No reads should be performed.
    for i in range(5):
        storage.read()
        assert storage.reindex_count == 0
        assert storage.append_count == 0
        assert storage.write_count == 0

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


def test_read_on_empty_file(tmpdir, csv_storage_with_counters):
    """Test read method on empty file."""
    path = os.path.join(tmpdir, "test.csv")

    # Init storage object.  No reads should be performed.
    storage = csv_storage_with_counters(path)

    # Read and assert empty list.
    assert storage.read() == []
    assert storage.read() == []


def test_deserialization(tmpdir):
    """Test deserialization methods."""
    path = os.path.join(tmpdir, "test.csv")
    db = TinyFlux(path)

    t = datetime.now(timezone.utc)
    p = Point(time=t, measurement="a")
    db.insert(p)

    serialized_point = db.storage._serialize_point(p)

    assert db.storage._deserialize_measurement(serialized_point) == "a"
    assert db.storage._deserialize_timestamp(serialized_point) == t.replace(
        tzinfo=None
    )
    assert db.storage._deserialize_storage_item(serialized_point) == p

    db = TinyFlux(storage=MemoryStorage)

    db.insert(p)

    serialized_point = db.storage._serialize_point(p)

    assert db.storage._deserialize_measurement(serialized_point) == "a"
    assert db.storage._deserialize_timestamp(serialized_point) == t
    assert db.storage._deserialize_storage_item(serialized_point) == p


def test_flush_on_insert(tmpdir):
    """Test the flush_on_insert argument."""
    path = os.path.join(tmpdir, "test.csv")
    db = TinyFlux(path, flush_on_insert=True)
    assert db.storage._flush_on_insert
    p1 = Point()
    db.insert(p1)
    db.close()

    db = TinyFlux(path, flush_on_insert=False)
    assert not db.storage._flush_on_insert
    p2 = Point()
    db.insert(p2)
    assert db.all() == [p1, p2]


def test_write(tmpdir):
    """Test write method."""
    path = os.path.join(tmpdir, "test.csv")
    storage = CSVStorage(path)
    t = datetime.now(timezone.utc)
    p1 = Point(time=t)

    storage._write([storage._serialize_point(p1)])
    assert storage.read() == [p1]


def test_temporary_storage(tmpdir):
    """Test the temporary storage component of Storage class."""
    path = os.path.join(tmpdir, "test.csv")
    storage = CSVStorage(path)

    # Exception should be thrown if temp storage not initialized.
    with pytest.raises(IOError):
        storage.append([], temporary=True)
