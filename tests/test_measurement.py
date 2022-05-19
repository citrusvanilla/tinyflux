"""Tests for the tinyflux.measurement module.

Tests are generally organized by Measurement class method.
"""
from datetime import datetime, timezone, timedelta
import re
from typing import Generator

import pytest

from tinyflux import Point
from tinyflux.database import TinyFlux
from tinyflux.queries import FieldQuery, MeasurementQuery, TagQuery, TimeQuery
from tinyflux.storages import MemoryStorage


def test_init():
    """Test __init__ of Measurement instance."""
    db = TinyFlux(storage=MemoryStorage)
    m = db.measurement("a")
    m2 = db.measurement("b")

    assert m._name == "a"
    assert m2._name == "b"
    assert db.storage == m.storage == m2.storage
    assert db.index == m.index == m2.index


def test_iter():
    """Test __iter__ method of Measurement instance."""
    db = TinyFlux(storage=MemoryStorage)
    db.insert(Point())

    m = db.measurement("a")
    m.insert(Point())

    assert isinstance(m.__iter__(), Generator)
    for p in m:
        assert isinstance(p, Point)
        assert p.measurement == m._name

    assert len(m.all()) == 1


def test_len():
    """Test __len__ of Measuremnt."""
    db = TinyFlux(storage=MemoryStorage)
    m = db.measurement("a")
    assert len(m) == 0

    # Insert point.
    m.insert(Point())
    assert m.index.valid and len(m.index) == 1
    assert len(m.index.get_measurements()) == 1

    # Insert point out of order.
    m.insert(Point(time=datetime.now(timezone.utc) - timedelta(days=365)))
    assert not m.index.valid

    # Get len again
    assert len(m) == 2


def test_repr():
    """Test __repr__ of Measurement."""
    db = TinyFlux(storage=MemoryStorage)
    name = "a"
    m = db.measurement(name)

    assert re.match(
        r"<Measurement name=a, "
        r"total=0, "
        r"storage=<tinyflux\.storages\.MemoryStorage object at [a-zA-Z0-9]+>>",
        repr(m),
    )

    m.insert(Point())

    assert re.match(
        r"<Measurement name=a, "
        r"total=1, "
        r"storage=<tinyflux\.storages\.MemoryStorage object at [a-zA-Z0-9]+>>",
        repr(m),
    )

    db = TinyFlux(auto_index=False, storage=MemoryStorage)
    name = "b"
    m = db.measurement(name)

    assert re.match(
        r"<Measurement name=b, "
        r"storage=<tinyflux\.storages\.MemoryStorage object at [a-zA-Z0-9]+>>",
        repr(m),
    )


def test_name():
    """Test name property of Measurement instance."""
    db = TinyFlux(storage=MemoryStorage)
    m = db.measurement("a")
    assert m.name == m._name == "a"


def test_storage():
    """Test storage property of Measurement instance."""
    db = TinyFlux(storage=MemoryStorage)
    m1 = db.measurement("a")
    m2 = db.measurement("b")
    assert m1.storage == m2.storage == db.storage


def test_all():
    """Test fetching all points from a measurement."""
    db = TinyFlux(storage=MemoryStorage)
    p1 = Point(tags={"a": "1"})
    p2 = Point(tags={"a": "2"})
    m = db.measurement("s")

    assert m.all() == []

    m.insert(p1)
    m.insert(p2)
    assert m.all() == [p1, p2]

    p3 = Point(time=datetime.now(timezone.utc) - timedelta(days=10))
    m.insert(p3)

    # Test "sorted" argument.
    assert m.all(sorted=False) == [p1, p2, p3]
    assert m.all(sorted=True) == [p3, p1, p2]


def test_contains():
    """Test the contains method of the Measurement class."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m1 = db.measurement("m1")
    m2 = db.measurement("m2")

    m1.insert(Point(tags={"a": "A"}, fields={"a": 1}))
    m1.insert(Point(tags={"a": "A"}, fields={"a": 2}))
    m2.insert(Point(tags={"b": "B"}, fields={"b": 1}))
    m2.insert(Point(tags={"b": "B"}, fields={"b": 2}))

    # Test contains with invalid invocation.
    with pytest.raises(TypeError):
        m1.contains()

    # Valid index, no query items.
    db.reindex()
    assert not m1.contains(TagQuery().b.exists())
    assert not m2.contains(TagQuery().a.exists())

    # Valid index, complete query with items.
    assert m1.contains(TagQuery().a.exists())
    assert m2.contains(TagQuery().b.exists())
    assert m1.contains(FieldQuery().a == 2)
    assert not m1.contains(FieldQuery().a == 3)
    assert m2.contains(FieldQuery().b == 1)
    assert not m2.contains(FieldQuery().b == 3)

    # Test with invalid index.
    m1.insert(
        Point(
            time=datetime.now(timezone.utc) - timedelta(days=365),
            tags={"a": "c"},
            fields={"a": 3},
        )
    )
    assert not db.index.valid
    assert m1.contains(FieldQuery().a == 3)
    assert not m2.contains(FieldQuery().a == 3)


def test_count():
    """Test the count method of the Measurement class."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m1 = db.measurement("m1")
    m2 = db.measurement("m2")

    m1.insert(Point(tags={"a": "A"}, fields={"a": 1}))
    m1.insert(Point(tags={"a": "A"}, fields={"b": 2}))
    m2.insert(Point(tags={"b": "B"}, fields={"a": 1}))
    m2.insert(Point(tags={"b": "B"}, fields={"b": 2}))

    # Valid index, no items.
    db.reindex()
    assert not m1.count(TagQuery().ummm == "okay")
    assert not m2.count(TagQuery().ummm == "okay")

    # Valid index, complete index query.
    assert m1.count(TagQuery().a == "A") == 2
    assert m2.count(TagQuery().b == "B") == 2
    assert m1.count(FieldQuery().a == 1) == 1
    assert m2.count(FieldQuery().b == 3) == 0

    # Invalid index.
    m1.insert(
        Point(
            time=datetime.now(timezone.utc) - timedelta(days=365),
            tags={"a": "b", "c": "d"},
            fields={"e": 1, "f": 3},
        )
    )
    assert not db.index.valid
    assert m1.count(FieldQuery().e == 1) == 1


def test_get():
    """Test the get method of the Measurement class."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m1 = db.measurement("m1")
    m2 = db.measurement("m2")

    p1 = Point(tags={"a": "A"}, fields={"a": 1})
    p2 = Point(tags={"a": "A"}, fields={"b": 2})
    p3 = Point(tags={"b": "B"}, fields={"a": 1})
    p4 = Point(tags={"b": "B"}, fields={"b": 2})

    m1.insert(p1)
    m1.insert(p2)
    m2.insert(p3)
    m2.insert(p4)

    # Valid index, no items.
    db.reindex()
    assert not m1.get(TagQuery().ummm == "okay")
    assert not m2.get(TagQuery().ummm == "okay")

    # Valid index, complete index query.
    assert m1.get(TagQuery().a == "A") == p1
    assert m2.get(TagQuery().b == "B") == p3
    assert m1.get(FieldQuery().a == 1) == p1
    assert not m2.get(FieldQuery().b == 3)

    # Invalid index.
    p5 = Point(
        time=datetime.now(timezone.utc) - timedelta(days=365),
        tags={"a": "b", "c": "d"},
        fields={"e": 1, "f": 3},
    )
    m1.insert(p5)
    assert not db.index.valid
    assert m1.get(FieldQuery().e == 1) == p5


def test_get_field_keys():
    """Test show field keys."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m = db.measurement("_default")
    m2 = db.measurement("some_missing_measurement")
    assert db.index.valid

    # Valid index, nothing in storage/index.
    assert m.get_field_keys() == []

    m.insert(Point())
    db.reindex()
    assert m.get_field_keys() == []
    assert m2.get_field_keys() == []

    m.insert(Point(fields={"a": 1}))
    db.reindex()
    assert m.get_field_keys() == ["a"]
    assert m2.get_field_keys() == []

    m.insert(Point(fields={"a": 2, "b": 3}))
    db.reindex()
    assert m.get_field_keys() == ["a", "b"]
    assert m2.get_field_keys() == []

    # Invalidate index.
    m.insert(
        Point(
            time=datetime.now(timezone.utc) - timedelta(days=1),
            fields={"a": 1, "c": 3},
        )
    )

    assert m.get_field_keys() == ["a", "b", "c"]
    assert m2.get_field_keys() == []


def test_get_field_values():
    """Test show field values."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m = db.measurement("_default")
    m2 = db.measurement("some_missing_measurement")
    assert db.index.valid

    # Valid index, nothing in storage/index.
    assert m.get_field_values("a") == []

    m.insert(Point())
    db.reindex()
    assert m.get_field_values("a") == []
    assert m2.get_field_values("a") == []

    m.insert(Point(fields={"a": 1}))
    db.reindex()
    assert m.get_field_values("a") == [1]
    assert m2.get_field_values("a") == []

    m.insert(Point(fields={"a": 2, "b": 3}))
    db.reindex()
    assert m.get_field_values("a") == [1, 2]
    assert m2.get_field_values("b") == []

    # Invalidate index.
    m.insert(
        Point(
            time=datetime.now(timezone.utc) - timedelta(days=1),
            fields={"a": 1, "c": 3},
        )
    )

    assert m.get_field_values("a") == [1, 2, 1]
    assert m2.get_field_values("a") == []


def test_get_tag_keys():
    """Test show tag keys."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m = db.measurement("_default")
    m2 = db.measurement("some_missing_measurement")
    assert db.index.valid

    # Valid index, nothing in storage/index.
    assert m.get_tag_keys() == []

    m.insert(Point())
    db.reindex()
    assert m.get_tag_keys() == []
    assert m2.get_tag_keys() == []

    m.insert(Point(tags={"a": "1"}))
    db.reindex()
    assert m.get_tag_keys() == ["a"]
    assert m2.get_tag_keys() == []

    m.insert(Point(tags={"a": "2", "b": "3"}))
    db.reindex()
    assert m.get_tag_keys() == ["a", "b"]
    assert m2.get_tag_keys() == []

    # Invalidate index.
    m.insert(
        Point(
            time=datetime.now(timezone.utc) - timedelta(days=1),
            tags={"a": "1", "c": "3"},
        )
    )

    assert m.get_tag_keys() == ["a", "b", "c"]
    assert m2.get_tag_keys() == []


def test_get_tag_values():
    """Test show tag keys."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m = db.measurement("_default")
    m2 = db.measurement("other_measurement")
    m3 = db.measurement("missing_measurement")
    assert db.index.valid

    # Valid index, nothing in storage/index.
    assert m.get_tag_values() == {}
    assert m2.get_tag_values() == {}

    m.insert(Point())
    m2.insert(Point())
    db.reindex()
    assert m.get_tag_values() == {}
    assert m2.get_tag_values() == {}

    m.insert(Point(tags={"a": "1"}))
    m2.insert(Point(tags={"a": "horse"}))
    db.reindex()
    assert m.get_tag_values() == {"a": ["1"]}
    assert m.get_tag_values(["a"]) == {"a": ["1"]}
    assert m.get_tag_values(["b"]) == {"b": []}
    assert m2.get_tag_values() == {"a": ["horse"]}
    assert m2.get_tag_values(["a"]) == {"a": ["horse"]}
    assert m2.get_tag_values(["b"]) == {"b": []}

    m.insert(Point(tags={"a": "1", "b": "2"}))
    m2.insert(Point(tags={"a": "cow", "b": "bird"}))
    db.reindex()
    assert m.get_tag_values() == {"a": ["1"], "b": ["2"]}
    assert m.get_tag_values(["a"]) == {"a": ["1"]}
    assert m.get_tag_values(["b"]) == {"b": ["2"]}
    assert m.get_tag_values(["c"]) == {"c": []}
    assert m.get_tag_values(["a", "b"]) == {"a": ["1"], "b": ["2"]}
    assert m.get_tag_values(["a", "c"]) == {"a": ["1"], "c": []}
    assert m2.get_tag_values() == {"a": ["cow", "horse"], "b": ["bird"]}
    assert m3.get_tag_values(["a", "b"]) == {"a": [], "b": []}
    assert m3.get_tag_values() == {}

    # Invalidate index.
    m.insert(
        Point(
            time=datetime.now(timezone.utc) - timedelta(days=1),
            tags={"a": "a", "c": "3"},
        )
    )

    assert m.get_tag_values() == {"a": ["1", "a"], "b": ["2"], "c": ["3"]}
    assert m.get_tag_values(["c"]) == {"c": ["3"]}
    assert m.get_tag_values(["d"]) == {"d": []}
    assert m.get_tag_values(["a", "b"]) == {"a": ["1", "a"], "b": ["2"]}
    assert m.get_tag_values(["c", "d"]) == {"c": ["3"], "d": []}
    assert m2.get_tag_values() == {"a": ["cow", "horse"], "b": ["bird"]}


def test_get_timestamps():
    """Test get timestamps."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m1 = db.measurement("_default")
    m2 = db.measurement("other_measurement")
    assert db.index.valid

    # Valid index, nothing in storage/index.
    assert m1.get_timestamps() == []
    assert m2.get_timestamps() == []

    t1 = datetime.now(timezone.utc)
    m1.insert(Point(time=t1))
    db.reindex()
    assert m1.get_timestamps() == [t1]
    assert m2.get_timestamps() == []

    t2 = t1 + timedelta(days=1)
    m1.insert(Point(time=t2))
    db.reindex()
    assert m1.get_timestamps() == [t1, t2]
    assert m2.get_timestamps() == []

    t3 = t2 + timedelta(days=1)
    m2.insert(Point(time=t3))
    db.reindex()
    assert m1.get_timestamps() == [t1, t2]
    assert m2.get_timestamps() == [t3]

    # Invalidate index.
    t4 = t1 - timedelta(days=1)
    db.insert(Point(time=t4))

    assert m1.get_timestamps() == [t1, t2, t4]
    assert m2.get_timestamps() == [t3]


def test_insert():
    """Test the insert method of the Measurement class."""
    db = TinyFlux(storage=MemoryStorage)

    # Assert exception with non-point insert.
    with pytest.raises(TypeError, match="Data must be a Point instance."):
        db.measurement("a").insert({})

    # Some measurements.
    m1, m2 = db.measurement("a"), db.measurement("b")
    p1, p2 = Point(), Point()

    # Insert in-order. Index should be valid.
    assert m1.insert(p1) == 1
    assert db.index.valid and not db.index.empty
    assert len(db.index) == 1
    assert len(db) == 1
    assert len(m1) == 1
    assert len(m2) == 0

    # Insert in-order, into a different measurment.
    assert m2.insert(p2) == 1
    assert db.index.valid and not db.index.empty
    assert len(db.index) == 2
    assert len(db) == 2
    assert len(m1) == 1
    assert len(m2) == 1

    # Insert out-of-order.  Index should be invalid.
    assert (
        m1.insert(Point(time=datetime.now(timezone.utc) - timedelta(days=1)))
        == 1
    )
    assert not db.index.valid
    assert len(db.index) == 0
    assert len(m1) == 2
    assert len(m2) == 1
    assert len(db) == 3


def test_insert_multiple():
    """Test the insert multiple method of the Measurement class."""
    db = TinyFlux(storage=MemoryStorage)

    # Some measurements.
    m1, m2 = db.measurement("a"), db.measurement("b")
    p1, p2 = Point(), Point()
    assert m1.insert_multiple(i for i in [p1, p2]) == 2

    assert len(m1) == 2
    assert len(m2) == 0
    assert m1.all() == [p1, p2]
    assert len(m2) == 0
    assert m2.all() == []

    # Some measurements with different measurement names.
    p3, p4 = Point(measurement="c"), Point(measurement="d")
    assert m2.insert_multiple([p3, p4]) == 2

    assert len(m1) == 2
    assert len(m2) == 2
    assert p3.measurement == "b"
    assert p4.measurement == "b"

    # Generator.
    assert m2.insert_multiple(Point() for _ in range(2)) == 2
    assert len(m2) == 4

    # Assert exception with non-point insert.
    with pytest.raises(TypeError, match="Data must be a Point instance."):
        db.measurement("a").insert_multiple([Point(), 1])


def test_remove():
    """Test the remove method of the Measurement class."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m1 = db.measurement("m1")
    m2 = db.measurement("m2")

    m1.insert(Point(tags={"a": "A"}, fields={"a": 1}))
    m1.insert(Point(tags={"a": "A"}, fields={"b": 2}))
    m1.insert(Point(tags={"a": "AA"}, fields={"b": 3}))
    m2.insert(Point(tags={"b": "B"}, fields={"a": 1}))
    m2.insert(Point(tags={"b": "B"}, fields={"b": 2}))
    db.reindex()

    # Valid index, no items.
    assert not m1.remove(TagQuery().c == "C")
    assert not m2.remove(TagQuery().c == "C")
    assert m1.remove(FieldQuery().a == 1) == 1
    db.reindex()
    assert m2.remove(FieldQuery().a == 1) == 1
    db.reindex()
    assert len(m1) == 2
    assert len(m2) == 1
    assert db.index.valid
    assert len(db.index) == len(db) == 3

    # Valid index, complete query.
    assert m1.remove(FieldQuery().b == 2) == 1
    assert len(m1) == 1
    assert m1.remove(TagQuery().a == "AA") == 1
    assert len(m1) == 0

    # Remove last item in db.
    assert m2.remove(TagQuery().b == "B") == 1
    db.reindex()
    assert db.index.valid
    assert db.index.empty
    assert len(db) == 0

    # Insert points out-of-order.
    m1.insert(
        Point(
            time=datetime.now(timezone.utc) - timedelta(days=1),
            tags={"a": "AA"},
            fields={"a": 3},
        )
    )
    m2.insert(
        Point(
            time=datetime.now(timezone.utc),
            tags={"a": "AA"},
            fields={"a": 3},
        )
    )
    m1.insert(Point(fields={"a": 4}))
    assert not db.index.valid
    assert db.index.empty

    # Invalid index. Remove 1 item.
    assert m1.remove(FieldQuery().a == 3) == 1
    db.reindex()
    assert db.index.valid
    assert not db.index.empty
    assert len(m1) == 1
    assert len(db) == 2
    assert m1.remove(FieldQuery().a == 4) == 1

    # Remove last item.
    assert m2.remove(FieldQuery().a == 3) == 1
    db.reindex()
    assert db.index.valid
    assert db.index.empty
    assert len(m2) == 0
    assert len(db) == 0


def test_remove_all():
    """Test the remove all method of the Measurement class."""
    db = TinyFlux(storage=MemoryStorage)
    m1 = db.measurement("m1")
    m2 = db.measurement("m2")
    m3 = db.measurement("m3")

    m1.insert(Point())
    m2.insert(Point())
    m1.insert(Point())

    # Valid index, no index items.
    assert not m3.remove_all()
    assert db.index.valid

    # Valid index, non-empty query candidate set.
    assert m2.remove_all() == 1
    assert db.index.valid
    assert len(db.index) == 2
    assert not len(m2)

    # Invalidate the index.
    m2.insert(
        Point(
            time=datetime.now(timezone.utc) - timedelta(days=365),
            measurement="m1",
        )
    )
    assert not db.index.valid
    assert len(m2) == 1

    # Invalid index.
    assert m1.remove_all() == 2
    assert db.index.valid
    assert len(db) == 1

    # Drop only remaining measurement.
    assert m2.remove_all()
    assert db.index.valid
    assert db.index.empty
    assert not len(db)


def test_search():
    """Test the search method of the Measurement class."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m1 = db.measurement("m1")
    m2 = db.measurement("m2")

    t = datetime.now(timezone.utc)

    p1 = Point(time=t, tags={"a": "A"}, fields={"a": 1})
    p2 = Point(time=t, tags={"a": "A"}, fields={"b": 2})
    p3 = Point(time=t - timedelta(days=1), tags={"a": "AA"}, fields={"b": 3})

    p4 = Point(time=t, tags={"b": "B"}, fields={"a": 1})
    p5 = Point(time=t, tags={"b": "B"}, fields={"b": 2})

    m1.insert(p1)
    m1.insert(p2)

    m2.insert(p4)
    m2.insert(p5)

    # Valid index, no items.
    db.reindex()
    assert not m1.search(TagQuery().c == "B")
    assert not m2.search(TagQuery().c == "B")

    # Valid index, complete index search.
    assert m1.search(TagQuery().a == "A") == [p1, p2]
    assert m2.search(TagQuery().b == "B") == [p4, p5]
    assert m1.search(FieldQuery().a == 1) == [p1]
    assert m2.search(FieldQuery().b == 2) == [p5]

    # Invalidate the index.
    m1.insert(p3)
    assert not db.index.valid

    # Invalid index search.
    assert m1.search(TimeQuery() < t) == [p3]

    # Reindex.
    db.reindex()
    assert db.index.valid
    assert not db.index.empty

    # Search by measurement.
    assert m1.search(MeasurementQuery() == "_default") == []
    assert not m1.search(MeasurementQuery() != "_default") == [
        p3,
        p1,
        p2,
        p4,
        p5,
    ]

    # Search by time.
    assert m1.search(TimeQuery() < t) == [p3]
    assert m1.search(TimeQuery() <= t, sorted=True) == [p3, p1, p2]
    assert m1.search(TimeQuery() <= t, sorted=False) == [p1, p2, p3]
    assert m2.search(TimeQuery() == t) == [p4, p5]
    assert m2.search(TimeQuery() > t) == []
    assert m2.search(TimeQuery() >= t) == [p4, p5]

    # Search with a query that has a path.
    assert m1.search(TagQuery().a.exists()) == [p3, p1, p2]
    assert m2.search(FieldQuery().a.exists()) == [p4]


def test_select():
    """Test select method."""
    db = TinyFlux(storage=MemoryStorage, auto_index=False)
    m1 = db.measurement("m1")

    t = datetime.now(timezone.utc)

    p1 = Point(time=t, tags={"a": "A"}, fields={"a": 1})
    p2 = Point(time=t, tags={"a": "A"}, fields={"b": 2})

    m1.insert(p1)
    m1.insert(p2)

    db.reindex()

    # Valid index.
    rst = m1.select(
        ("measurement", "time", "tags.a", "fields.a"),
        MeasurementQuery().noop(),
    )

    assert rst == [("m1", t, "A", 1), ("m1", t, "A", None)]

    # Invalidate index.
    t2 = t - timedelta(hours=1)

    m1.insert(Point(measurement="m1", time=t2, tags={"b": "B"}))

    rst = db.select(
        ("measurement", "time", "tags.b", "fields.a"),
        MeasurementQuery().noop(),
    )

    assert rst == [
        ("m1", t, None, 1),
        ("m1", t, None, None),
        ("m1", t2, "B", None),
    ]

    assert (
        db.measurement("m2").select("measurement", MeasurementQuery() == "m2")
        == []
    )

    # Bad select args.
    with pytest.raises(ValueError):
        db.select(("timestamp"), TagQuery().noop())

    with pytest.raises(ValueError):
        db.select(("tags."), TagQuery().noop())

    with pytest.raises(ValueError):
        db.select(("fields."), TagQuery().noop())


def test_update():
    """Test the update method of the Measurement class."""
    # Open up the DB with TinyFlux.
    db = TinyFlux(auto_index=False, storage=MemoryStorage)

    # Some mock points.
    t1 = datetime.now(timezone.utc) - timedelta(days=10)
    t2 = datetime.now(timezone.utc)
    t3 = datetime.now(timezone.utc) + timedelta(days=10)

    p1 = Point(time=t1, tags={"tk1": "tv1"}, fields={"fk1": 1})
    p2 = Point(time=t2, tags={"tk2": "tv2"}, fields={"fk2": 2})
    p3 = Point(time=t3, tags={"tk3": "tv3"}, fields={"fk3": 3})

    m1 = db.measurement("a")
    m2 = db.measurement("b")
    m3 = db.measurement("c")

    # Insert points into DB.
    m1.insert(p1)
    m2.insert(p2)
    m3.insert(p3)

    assert not db.index.valid
    assert len(db) == 3

    # Bad invocation.
    with pytest.raises(
        ValueError,
        match="Must include time, measurement, tags, and/or fields.",
    ):
        m1.update(TagQuery().tk1 == "tv1")

    with pytest.raises(
        ValueError,
        match="Argument 'query' must be a TinyFlux Query.",
    ):
        m1.update(3, tags={"a": "b"})

    with pytest.raises(
        ValueError, match="Tag set must contain only string values."
    ):
        m1.update(TagQuery().noop(), tags={"a": 1})

    with pytest.raises(
        ValueError, match="Field set must contain only numeric values."
    ):
        m1.update(TagQuery().noop(), fields={"a": "a"})

    # Valid index, no index results.
    db.reindex()
    assert not m3.update(TagQuery().tk1 == "tv1", tags={"tk1": "tv1"})

    # Valid index, complete search.
    assert m1.update(MeasurementQuery() == "a", fields={"fk1": 2}) == 1
    assert m1.get(FieldQuery().fk1 == 2) == p1
    assert p1.fields["fk1"] == 2
    assert m1.update(FieldQuery().fk1 == 2, fields={"fk1": 1}) == 1
    assert m1.update(FieldQuery().fk1 == 20, fields={"fk1": 1}) == 0
    assert m1.search(FieldQuery().fk1 == 1) == [p1]
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

    # Update with no found matches. DB will NOT reindex.
    assert m2.update(FieldQuery().fk2 == 1000, fields={"fk2": 4}) == 0

    # Update with found matches.
    assert m2.update(TagQuery().tk2 == "tv2", fields={"fk2": 4}) == 1
    assert m2.count(FieldQuery().fk2.exists()) == 1

    # Update should not reindex the db.
    assert not db.index.valid
    assert db.index.empty

    # Update with callables.
    rst = m1.update(
        FieldQuery().fk1.exists(),
        time=lambda x: x - timedelta(days=730),
        measurement=lambda _: "m0",
        tags=lambda x: {**x, "tk1": "tv10"} if x["tk1"] == "tv1" else x,
        fields=lambda x: {"fk2": x["fk1"] * 2} if "fk1" in x else {},
    )
    assert rst == 1

    m0 = db.measurement("m0")
    assert m0.count(TimeQuery() == t1 - timedelta(days=730)) == 1
    assert m0.count(MeasurementQuery() == "m0") == 1
    assert m0.count(TagQuery().tk1 == "tv10") == 1
    assert m0.count(FieldQuery().fk2 == 2) == 1

    # Update with time.
    assert m0.update_all(time=t1) == 1

    assert m0.count(TimeQuery() == t1) == 1

    # Update with measurement.
    m0.update_all(measurement="m1")
    assert m0.count(TimeQuery() == t1) == 0


def test_auto_index_off():
    """Test the behavior of the Measurement class without auto-indexing."""
    # Open up the DB with TinyFlux.
    db = TinyFlux(auto_index=True, storage=MemoryStorage)

    p = Point(tags={"tk": "tv"}, fields={"fk": 1})
    m = db.measurement("m")
    q1 = TagQuery().tk == "tv"
    q2 = FieldQuery().fk == 1

    m.insert(p)
    assert m.all() == [p]
    assert m.update_all(fields={"fk": 2}) == 1
    assert m.get(q2) is None
    assert m.get(~q2) == p
    assert m.get(q1) == p
