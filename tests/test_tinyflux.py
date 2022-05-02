"""Tests for the tinyflux.database module.

Tests are generally organized by TinyFlux class method.
"""
import csv
from datetime import datetime, timedelta
import os
from pathlib import Path
import re

import pytest

from tinyflux import (
    FieldQuery,
    MeasurementQuery,
    TagQuery,
    TimeQuery,
    TinyFlux,
    Point,
)
from tinyflux.measurement import Measurement
from tinyflux.storages import MemoryStorage, CSVStorage


def test_init(tmpdir):
    """Test index and auto_index properties."""
    # Bad invocation.
    with pytest.raises(TypeError, match="'auto_index' must be True/False."):
        TinyFlux(auto_index="True", storage=MemoryStorage)

    # Test storage instances.
    db_ = TinyFlux(storage=MemoryStorage)
    assert isinstance(db_.storage, MemoryStorage)

    path = os.path.join(tmpdir, "test.csv")
    db_ = TinyFlux(path, storage=CSVStorage)
    assert isinstance(db_.storage, CSVStorage)

    # Auto index ON.
    db = TinyFlux(storage=MemoryStorage)

    assert db._auto_index
    assert db.index is not None
    assert db.index.valid
    assert db.index.empty

    # Auto index OFF.
    db = TinyFlux(auto_index=False, storage=MemoryStorage)
    assert not db._auto_index
    assert db.index.empty

    db.insert(Point())

    assert not db._auto_index
    assert db.index.empty

    db.all()

    assert not db._auto_index
    assert db.index.empty


def test_len_with_index():
    """Test __len__ when auto_index is True."""
    db = TinyFlux(auto_index=True, storage=MemoryStorage)
    t_now = datetime.utcnow()

    db.insert(Point(time=t_now))
    assert db._index.valid
    assert len(db._index) == 1
    assert len(db) == 1

    db.insert(Point(time=t_now + timedelta(days=10)))
    assert db._index.valid
    assert len(db._index) == 2
    assert len(db) == 2

    db.insert(Point(time=t_now - timedelta(days=10)))
    assert not db._index.valid
    assert len(db._index) == 0
    assert len(db) == 3

    db.insert(Point(time=t_now - timedelta(days=5)))
    assert not db._index.valid
    assert len(db._index) == 0
    assert len(db) == 4


def test_len_without_index():
    """Test __len__ when auto_index is False."""
    db = TinyFlux(auto_index=False, storage=MemoryStorage)
    t_now = datetime.utcnow()

    assert not len(db)
    db.insert(Point(time=t_now))
    assert not db._auto_index
    assert db.index.empty
    assert len(db) == 1

    db.insert(Point(time=t_now + timedelta(days=10)))
    assert not db._auto_index
    assert db.index.empty
    assert len(db) == 2

    db.insert(Point(time=t_now - timedelta(days=10)))
    assert not db._auto_index
    assert db.index.empty
    assert len(db) == 3

    db.insert(Point(time=t_now - timedelta(days=5)))
    assert not db._auto_index
    assert db.index.empty
    assert len(db) == 4


def test_iter():
    """Test __iter__ method."""
    db = TinyFlux(storage=MemoryStorage)

    assert [i for i in db] == db.all()


def test_repr(tmpdir: Path):
    """Test __repr__ method."""
    path = os.path.join(tmpdir, "test.csv")

    db = TinyFlux(path)

    assert re.match(
        r"<TinyFlux "
        r"all_points_count=0, "
        r"auto_index_ON=True, "
        r"index_valid=True>",
        repr(db),
    )

    db.insert(Point())

    assert re.match(
        r"<TinyFlux "
        r"all_points_count=1, "
        r"auto_index_ON=True, "
        r"index_valid=True>",
        repr(db),
    )

    db.insert(Point(time=datetime.utcnow() - timedelta(days=365)))

    assert re.match(
        r"<TinyFlux " r"auto_index_ON=True, " r"index_valid=False>",
        repr(db),
    )


def test_all():
    """Test all method."""
    db = TinyFlux(storage=MemoryStorage)

    # Drop all and add new points.
    db.drop_measurements()

    for i in range(10):
        db.insert(
            Point(
                fields={"temperature": 60 + i},
                tags={"location": "office"},
            )
        )

    for i in range(10):
        db.insert(
            Point(
                measurement="new york",
                fields={"temperature": 30 + i},
                tags={"location": "office"},
            )
        )

    assert len(db.measurement("new york").all()) == 10
    assert len(db.measurement("_default").all()) == 10
    assert len(db.all()) == 20


def test_contains():
    """Test contains method."""
    db = TinyFlux(storage=MemoryStorage)

    # Test contains with invalid invocation.
    with pytest.raises(TypeError):
        db.contains()

    # Test with valid index and complete index result.
    db.insert(Point())
    db.insert_multiple(
        [Point(tags={"a": "b"}, fields={"a": 1}) for _ in range(3)]
    )
    assert db.index.valid
    assert db.contains(TagQuery().a == "b")
    assert not db.contains(TagQuery().a == "c")
    assert not db.contains(TagQuery().x == "z")

    # Test with valid index and incomplete index result.
    assert db.contains(FieldQuery().a > 0)
    assert not db.contains(FieldQuery().a > 10)

    # Test with invalid index.
    db.insert(
        Point(
            time=datetime.utcnow() - timedelta(days=365),
            tags={"a": "c"},
            fields={"a": 100},
        )
    )
    assert not db.index.valid
    assert db.contains(TagQuery().a == "c")
    assert not db.contains(TagQuery().a == "d")
    assert db.contains(FieldQuery().a > 99)
    assert not db.contains(FieldQuery().a > 1000)


def test_count():
    """Test count method."""
    db = TinyFlux(storage=MemoryStorage)

    # Insert some points.
    db.insert(
        Point(
            time=datetime.utcnow(),
            tags={"a": "b", "c": "d"},
            fields={"e": 1, "f": 2},
        )
    )

    db.insert(
        Point(
            time=datetime.utcnow(),
            tags={"a": "b", "c": "dd"},
            fields={"e": 1, "g": 3},
        )
    )

    # Valid index, no items.
    assert not db.count(TagQuery().ummm == "okay")

    # Valid index, complete index query.
    assert db.count(TagQuery().a == "b") == 2

    # Valid index, incomplete index query.
    assert db.count(FieldQuery().e == 1) == 2
    assert not db.count(FieldQuery().f == 1)
    assert db.count(FieldQuery().g == 3) == 1

    # Invalid index.
    db.insert(
        Point(
            time=datetime.utcnow() - timedelta(days=365),
            tags={"a": "b", "c": "d"},
            fields={"e": 1, "f": 3},
        )
    )
    assert db.count(FieldQuery().e == 1) == 3


def test_drop_measurement():
    """Test drop_measurement method."""
    db = TinyFlux(storage=MemoryStorage)

    db.insert(Point(measurement="m1"))
    db.insert(Point(measurement="m1"))
    db.insert(Point(measurement="m2"))

    # Valid index, no index candidates.
    assert not db.drop_measurement("m3")
    assert db.index.valid

    # Valid index, non-empty query candidate set.
    assert db.drop_measurement("m1") == 2
    assert db.index.valid

    # Invalidate the index.
    db.insert(
        Point(time=datetime.utcnow() - timedelta(days=365), measurement="m1")
    )
    assert not db.index.valid

    # Drop a measurement. Deletes will re-index the db.
    assert db.drop_measurement("m2") == 1
    assert db.index.valid

    # Drop only remaining measurement.
    assert db.drop_measurement("m1") == 1
    assert db.index.valid
    assert db.index.empty
    assert not len(db)


def test_drop_measurements():
    """Test drop_measurement method."""
    db = TinyFlux(storage=MemoryStorage)
    db.insert(Point())
    db.insert(Point(measurement="m"))
    assert db.index.valid
    assert len(db.measurements()) == 2

    # Valid index, drop all measurements.
    db.drop_measurements()
    assert len(db.measurements()) == 0
    assert db.index.valid
    assert db.index.empty


def test_get():
    """Test get method."""
    db = TinyFlux(storage=MemoryStorage)

    # Invalid invocation.
    with pytest.raises(TypeError):
        db.get()

    # Insert some points.
    p1 = Point(tags={"a": "A"}, fields={"a": 1})
    p2 = Point(tags={"a": "B"}, fields={"a": 2})
    p3 = Point(tags={"a": "C"}, fields={"b": 3})
    db.insert_multiple([p1, p2, p3])
    assert db.index.valid and not db.index.empty

    # Valid index, no candidates.
    assert not db.get(FieldQuery().c >= 0)

    # Valid index, candidates.
    assert db.get(TagQuery().a == "C") == p3
    assert db.get(FieldQuery().b == 3) == p3
    assert not db.get(TagQuery().a == "D")
    assert not db.get(FieldQuery().b > 3)

    # Invalidate index.
    db.insert(Point(time=datetime.utcnow() - timedelta(days=365)))
    assert not db.index.valid

    assert db.get(TagQuery().a == "C") == p3
    assert db.get(FieldQuery().b == 3) == p3
    assert not db.get(TagQuery().a == "D")
    assert not db.get(FieldQuery().b > 3)


def test_insert():
    """Test insert method."""
    db = TinyFlux(storage=MemoryStorage)

    # Invalid invocation.
    with pytest.raises(TypeError, match="Data must be a Point instance."):
        db.insert(object())

    with pytest.raises(TypeError):
        db.insert([1, 2, 3])

    with pytest.raises(TypeError):
        db.insert({"bark"})

    with pytest.raises(TypeError):
        db.insert(db)

    with pytest.raises(TypeError):
        db.insert_multiple(Point())

    # Insert into empty db.
    assert db.insert(Point()) == 1
    assert db.index.valid and not db.index.empty
    assert len(db.index) == 1
    assert len(db) == 1

    # Insert in-order. Index should be valid.
    assert db.insert(Point()) == 1
    assert db.index.valid and not db.index.empty
    assert len(db.index) == 2
    assert len(db) == 2

    # Insert out-of-order.  Index should be invalid.
    assert db.insert(Point(time=datetime.utcnow() - timedelta(days=1))) == 1
    assert not db.index.valid
    assert len(db.index) == 0
    assert len(db) == 3


def test_insert_on_existingdb(tmpdir):
    """Test insert on DB w/ CSV after reopening existing file."""
    path = os.path.join(tmpdir, "test.csv")

    db = TinyFlux(path)
    assert db.index.valid
    assert db.index.empty

    p1 = Point()
    p2 = Point()
    p3 = Point()

    # Insert a point into empty file
    assert db.insert(p1) == 1
    assert len(db) == 1
    assert db.index.valid
    assert len(db.index) == 1

    db.close()

    # Open it again. File is not empty, so index is invalid.
    db = TinyFlux(path)
    assert not db.index.valid

    # Insert points.
    assert db.insert_multiple([p2, p3]) == 2
    assert len(db) == 3
    assert not db.index.valid


def test_insert_multiple():
    """Test insert_multiple method."""
    db = TinyFlux(storage=MemoryStorage)

    # Invalid insert.
    with pytest.raises(TypeError, match="Data must be a Point instance."):
        db.insert_multiple([Point(), 3])

    db.insert_multiple([Point() for _ in range(2)])
    assert len(db) == 2

    # Insert multiple from generator function
    def generator():
        """Mock generator."""
        for _ in range(2):
            yield Point()

    db.insert_multiple(generator())
    assert len(db) == 4

    # Insert multiple from inline generator.
    db.insert_multiple(Point() for _ in range(2))
    assert len(db) == 6


def test_measurement():
    """Test measurement method."""
    # Empty db.  No actual measurements, no Measurement references.
    db = TinyFlux(storage=MemoryStorage)
    assert not db._measurements
    assert not db.measurements()

    # Create a reference to a measurment that does not exist.
    m = db.measurement("a")
    assert "a" in db._measurements
    assert not len(m)
    assert not db.measurements()

    # Add a point to the db.
    db.insert(Point())
    assert "_default" not in db._measurements
    assert db.measurements() == {"_default"}

    # Create a reference to a measurement that does exist.
    m2 = db.measurement("_default")
    assert "_default" in db._measurements
    assert len(m2) == 1

    assert isinstance(m, Measurement)
    assert isinstance(m2, Measurement)


def test_measurements():
    """Test measurements method."""
    # Empty DB.
    db = TinyFlux(storage=MemoryStorage)
    assert db.index.valid
    assert db.measurements() == set({})

    # DB with points and valid index.
    db.insert(Point())
    assert db.index.valid
    assert db.measurements() == {"_default"}

    # DB with points and invalid index.
    db.insert(
        Point(measurement="a", time=datetime.utcnow() - timedelta(days=365))
    )
    assert not db.index.valid
    assert db.measurements() == {"a", "_default"}
    assert not db.index.valid


def test_reindex(tmpdir, capsys):
    """Test storage initialization when auto_index is False."""
    # Some mock points.
    p1 = Point(time=datetime.utcnow() - timedelta(days=10))
    p2 = Point(time=datetime.utcnow())
    p3 = Point(time=datetime.utcnow() + timedelta(days=10))

    # Mock CSV store.  Insert points out of order.
    path = os.path.join(tmpdir, "test.csv")
    f = open(path, "w")
    w = csv.writer(f)
    w.writerow(p2._serialize())
    w.writerow(p1._serialize())
    f.close()

    # Open up the DB with TinyFlux.
    db = TinyFlux(path, auto_index=True, storage=CSVStorage)
    assert not db.storage._index_intact
    assert not db.index.valid
    assert db.index.empty

    # Append a point.
    db.insert(p3)
    assert not db.index.valid
    assert db.index.empty
    assert not db._storage._is_sorted()
    assert not db._storage._index_intact

    # Reindex.
    db.reindex()

    # Check storage layer is sorted.
    f = open(path, "r+")
    r = csv.reader(f)
    for row, point in zip(r, [p1, p2, p3]):
        assert tuple(row) == point._serialize()
        assert Point()._deserialize(row) == point
    f.close()

    assert db._storage._is_sorted()
    assert db._storage._index_intact

    # Check new index.
    assert db.index.valid
    assert not db.index.empty
    assert len(db.index) == 3
    assert db.index._timestamps == [i.time.timestamp() for  i in [p1, p2, p3]]
    assert db.index._measurements == {"_default": {0, 1, 2}}
    assert not db.index._tags
    assert not db.index._fields

    # Try to index again.
    db.reindex()
    captured = capsys.readouterr()
    assert captured.out == "Index already valid.\n"


def test_remove():
    """Test remove method."""
    db = TinyFlux(storage=MemoryStorage)
    db.insert(Point(tags={"a": "A"}, fields={"a": 1}))
    db.insert(Point(tags={"a": "AA"}, fields={"a": 2}))
    db.insert(Point(tags={"b": "B"}, fields={"b": 2}))

    # Valid index, no candidates.
    assert not db.remove(TagQuery().c == "C")
    assert db.index.valid
    assert len(db.index) == 3
    assert len(db) == 3

    # Valid index, complete index query.
    assert db.remove(TagQuery().b == "B") == 1
    assert db.index.valid
    assert len(db.index) == 2
    assert len(db) == 2

    # Valid index, incomplete query.
    assert db.remove(FieldQuery().a == 1) == 1
    assert db.index.valid
    assert len(db.index) == 1
    assert len(db) == 1

    # Insert a point out-of-order.
    db.insert(
        Point(
            time=datetime.utcnow() - timedelta(days=1),
            tags={"a": "AA"},
            fields={"a": 3},
        )
    )
    assert not db.index.valid
    assert db.index.empty
    assert len(db.index) == 0
    assert len(db) == 2

    # Invalid index. Remove 1 item. Deletes will reindex DB.
    assert db.remove(FieldQuery().a == 2) == 1
    assert len(db.index) == 1
    assert len(db) == 1
    assert db.index.valid
    assert not db.index.empty

    # Remove last item.
    assert db.remove(FieldQuery().a == 3) == 1
    assert db.index.valid
    assert len(db) == 0
    assert db.index.empty


def test_remove_all():
    """Test remove_all method."""
    db = TinyFlux(storage=MemoryStorage)
    db.insert(Point())
    db.insert(Point(measurement="m"))
    assert len(db) == 2

    # Valid index, drop all measurements.
    db.remove_all()
    assert len(db) == 0
    assert db.index.valid
    assert db.index.empty
    assert db.storage._index_intact


def test_search():
    """Test search method."""
    db = TinyFlux(storage=MemoryStorage)
    t = datetime.utcnow()
    p1, p2 = Point(time=t, tags={"a": "A"}), Point(time=t, fields={"a": 1})
    db.insert_multiple([p1, p2])

    # Valid index, no candidates.
    assert not db.search(TagQuery().b == "B")

    # Valid index, complete index search.
    assert db.search(TagQuery().a == "A") == [p1]

    # Valid index, incomplete index search.
    assert db.search(FieldQuery().a == 1) == [p2]

    # Invalidate the index.
    p3 = Point(time=t - timedelta(days=1))
    db.insert(p3)
    assert not db.index.valid

    # Invalid index search.
    assert db.search(TimeQuery() < t) == [p3]

    # Reindex.
    db.reindex()
    assert db.index.valid
    assert not db.index.empty

    # Search by measurement.
    assert db.search(MeasurementQuery() == "_default") == [p3, p1, p2]
    assert not db.search(MeasurementQuery() != "_default")

    # Search by time.
    assert db.search(TimeQuery() < t) == [p3]
    assert db.search(TimeQuery() <= t) == [p3, p1, p2]
    assert db.search(TimeQuery() == t) == [p1, p2]
    assert db.search(TimeQuery() > t) == []
    assert db.search(TimeQuery() >= t) == [p1, p2]

    # Search with a query that has a path.
    assert db.search(TagQuery().a.exists()) == [p1]
    assert db.search(FieldQuery().a.exists()) == [p2]


def test_update():
    """Test update method."""
    db = TinyFlux(storage=MemoryStorage)

    # Insert some points.
    t1, t2 = datetime.utcnow(), datetime.utcnow() + timedelta(days=1)

    p1 = Point(
        time=t1,
        measurement="m1",
        tags={"tk1": "tv1", "tk2": "tv2"},
        fields={"fk1": 1},
    )
    p2 = Point(
        time=t2,
        measurement="m2",
        tags={"tk1": "tv1", "tk2": "tv2"},
        fields={"fk1": 1},
    )
    db.insert(p1)
    db.insert(p2)

    assert len(db) == 2

    # Bad invocation.
    with pytest.raises(
        ValueError,
        match="Must include time, measurement, tags, and/or fields.",
    ):
        db.update(TagQuery().tk1 == "tv1")

    with pytest.raises(
        ValueError,
        match="Selector must be a query or None.",
    ):
        db.update(3, tags={"a": "b"})

    with pytest.raises(
        ValueError, match="Tag set must contain only string values."
    ):
        db.update(tags={"a": 1})

    with pytest.raises(
        ValueError, match="Field set must contain only numeric values."
    ):
        db.update(fields={"a": "a"})

    # Missing updates.
    with pytest.raises(
        ValueError,
        match="Must include time, measurement, tags, and/or fields.",
    ):
        db.update(TagQuery().city == "la")

    # Bad selector.
    with pytest.raises(ValueError, match="Selector must be a query or None."):
        db.update(selector="some invalid type", tags={"k": "v"})

    with pytest.raises(ValueError, match="Selector must be a query or None."):
        db.update(selector=lambda x: x, tags={"k": "v"})

    with pytest.raises(ValueError, match="Selector must be a query or None."):
        db.update(selector=True, tags={"k": "v"})

    # Valid index, no index results.
    assert not db.update(MeasurementQuery() == "m3", measurement="m4")

    # Valid index, complete search.
    assert db.update(MeasurementQuery() == "m1", fields={"fk1": 2}) == 1
    assert db.get(FieldQuery().fk1 == 2) == p1
    assert p1.fields["fk1"] == 2

    # Valid index, incomplete search.
    assert db.update(FieldQuery().fk1 == 2, fields={"fk1": 1}) == 1
    assert db.search(FieldQuery().fk1 == 1) == [p1, p2]
    assert p1.fields["fk1"] == 1

    # Invalidate the index.
    p3 = Point(
        time=t1 - timedelta(days=1),
        measurement="m2",
        tags={"tk1": "tv1", "tk2": "tv3"},
        fields={"fk1": 1},
    )
    db.insert(p3)
    assert not db.index.valid

    # Update.
    assert db.update(TagQuery().tk2 == "tv2", fields={"fk2": 2}) == 2
    assert db.count(FieldQuery().fk2.exists()) == 2

    # Update should reindex the db.
    assert db.index.valid
    assert len(db.index) == 3

    # Update with callables.
    rst = db.update(
        FieldQuery().fk1.exists(),
        time=lambda x: x - timedelta(days=730),
        measurement=lambda _: "m0",
        tags=lambda x: {**x, "tk1": "tv10"} if x["tk1"] == "tv1" else x,
        fields=lambda x: {"fk2": x["fk1"] * 2} if "fk1" in x else {},
    )
    assert rst == 3
    assert db.count(TimeQuery() == t2 - timedelta(days=730)) == 1
    assert db.count(MeasurementQuery() == "m0") == 3
    assert db.count(TagQuery().tk1 == "tv10") == 3
    assert db.count(FieldQuery().fk2 == 2) == 3


def test_update_all():
    """Test updating all using update method."""
    db = TinyFlux(storage=MemoryStorage)
    t = datetime.utcnow()

    # Insert some points.
    for i in ["lincoln heights", "santa monica", "monterrey park"]:
        db.insert(
            Point(
                time=t,
                tags={"city": "la", "neighborhood": i},
                fields={"temp_f": 70.0},
            )
        )

    # Update tags.
    assert db.update(tags={"country": "USA"}) == 3
    assert db.count(TagQuery().country == "USA") == 3

    # Update fields.
    assert db.update(fields={"temp_f": 60.0}) == 3
    assert db.count(FieldQuery().temp_f == 60.0) == 3

    # Update measurement.
    assert db.update(measurement="neighborhood temps") == 3
    assert db.count(MeasurementQuery() == "neighborhood temps") == 3

    # Update time.
    assert db.update(time=t - timedelta(days=1)) == 3
    assert db.count(TimeQuery() == t - timedelta(days=1)) == 3


def test_multipledbs():
    """Test inserting points into multiple DBs."""
    db1 = TinyFlux(storage=MemoryStorage)
    db2 = TinyFlux(storage=MemoryStorage)

    points = [
        Point(
            time=datetime.utcnow(),
            tags={"city": "los angeles", "neighborhood": "chinatown"},
            fields={"temp_f": 71.2 + i, "num_restaurants": 6 + i},
        )
        for i in range(3)
    ]

    db1.insert(points[0])
    db2.insert(points[0])

    assert db1.all() == db2.all()

    db1.insert(points[1])
    db2.insert(points[2])

    assert len(db1) == 2
    assert len(db2) == 2


def test_storage_index_initialization_with_autoindex_ON(tmpdir):
    """Test storage initialization when auto_index is False."""
    # Some mock points.
    t1 = datetime.utcnow() - timedelta(days=10)
    t2 = datetime.utcnow()
    t3 = datetime.utcnow() + timedelta(days=10)

    p1 = Point(time=t1)
    p2 = Point(time=t2)
    p3 = Point(time=t3)

    # Mock CSV store.  Insert points out of order.
    path = os.path.join(tmpdir, "test.csv")
    with open(path, "w") as f:
        w = csv.writer(f)
        w.writerow(p2._serialize())
        w.writerow(p1._serialize())

    # Open up the DB with TinyFlux.
    db = TinyFlux(path, auto_index=True, storage=CSVStorage)
    assert not db.storage._index_intact
    assert not db.index.valid
    assert db.index.empty

    # Append a point.
    db.insert(p3)
    assert not db.index.valid
    assert db.index.empty

    # Read all.
    db.all()
    assert not db.index.valid
    assert db.index.empty

    # Reindex.
    db.reindex()
    assert db.index.valid
    assert not db.index.empty


def test_open_unindexed_storage_with_autoindex_OFF(tmpdir):
    """Test opening existing data store with auto_index set to False."""
    # Some mock points.
    t1 = datetime.utcnow() - timedelta(days=10)
    t2 = datetime.utcnow()
    t3 = datetime.utcnow() + timedelta(days=10)

    p1 = Point(time=t1)
    p2 = Point(time=t2)
    p3 = Point(time=t3)

    # Mock CSV store.
    path = os.path.join(tmpdir, "test.csv")
    with open(path, "w") as f:
        w = csv.writer(f)
        w.writerow(p2._serialize())
        w.writerow(p1._serialize())

    # Open up the DB with TinyFlux.
    db = TinyFlux(path, auto_index=False, storage=CSVStorage)
    assert not db._index.valid
    assert db.index.empty

    # Append a point.
    db.insert(p3)
    assert not db._index.valid
    assert db.index.empty

    # Read all.
    db.all()
    assert not db._index.valid
    assert db.index.empty
