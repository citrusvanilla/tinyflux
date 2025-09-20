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


@pytest.fixture
def db():
    """Create a TinyFlux database with MemoryStorage."""
    return TinyFlux(storage=MemoryStorage)


@pytest.fixture
def db_no_index():
    """Create a TinyFlux database with MemoryStorage and auto_index=False."""
    return TinyFlux(storage=MemoryStorage, auto_index=False)


@pytest.fixture
def basic_measurements(db):
    """Create basic measurements m1 and m2 with some test data."""
    m1 = db.measurement("m1")
    m2 = db.measurement("m2")

    # Insert basic test data
    m1.insert(Point(tags={"a": "A"}, fields={"a": 1}))
    m1.insert(Point(tags={"a": "A"}, fields={"b": 2}))
    m2.insert(Point(tags={"b": "B"}, fields={"a": 1}))
    m2.insert(Point(tags={"b": "B"}, fields={"b": 2}))

    return db, m1, m2


@pytest.fixture
def test_times():
    """Generate standard test timestamps."""
    t_now = datetime.now(timezone.utc)
    return {
        "t_past": t_now - timedelta(days=10),
        "t_now": t_now,
        "t_future": t_now + timedelta(days=10),
        "t_yesterday": t_now - timedelta(days=1),
        "t_year_ago": t_now - timedelta(days=365),
    }


@pytest.fixture
def db_with_index():
    """Create a TinyFlux database with MemoryStorage and auto_index=True."""
    return TinyFlux(auto_index=True, storage=MemoryStorage)


@pytest.fixture
def batch_measurement(db_with_index):
    """Create a test measurement for batch testing."""
    return db_with_index.measurement("test_batch")


@pytest.fixture
def base_time():
    """Generate base time for batch testing."""
    return datetime.now(timezone.utc)


@pytest.fixture
def ordered_points(base_time):
    """Create 4 time-ordered points for testing."""
    points = []
    for i in range(4):
        point = Point(
            time=base_time + timedelta(seconds=i * 10),
            measurement="test_batch",
            tags={"device": f"device_{i}"},
            fields={"value": float(i)},
        )
        points.append(point)
    return points


@pytest.fixture
def out_of_order_points(base_time):
    """Create 4 points with some out of time order."""
    return [
        Point(
            time=base_time + timedelta(seconds=0),
            measurement="test_batch",
            tags={"device": "device_0"},
            fields={"value": 0.0},
        ),
        Point(
            time=base_time + timedelta(seconds=10),
            measurement="test_batch",
            tags={"device": "device_1"},
            fields={"value": 1.0},
        ),
        Point(
            time=base_time + timedelta(seconds=5),  # Out of order!
            measurement="test_batch",
            tags={"device": "device_2"},
            fields={"value": 2.0},
        ),
        Point(
            time=base_time + timedelta(seconds=20),
            measurement="test_batch",
            tags={"device": "device_3"},
            fields={"value": 3.0},
        ),
    ]


def test_init(db):
    """Test __init__ of Measurement instance."""
    m = db.measurement("a")
    m2 = db.measurement("b")

    assert m._name == "a"
    assert m2._name == "b"
    assert db.storage == m.storage == m2.storage
    assert db.index == m.index == m2.index


def test_iter(db):
    """Test __iter__ method of Measurement instance."""
    db.insert(Point())

    m = db.measurement("a")
    m.insert(Point())

    assert isinstance(m.__iter__(), Generator)
    for p in m:
        assert isinstance(p, Point)
        assert p.measurement == m._name

    assert len(m.all()) == 1


def test_len(db, test_times):
    """Test __len__ of Measurement."""
    m = db.measurement("a")
    assert len(m) == 0

    # Insert point.
    m.insert(Point())
    assert m.index.valid and len(m.index) == 1
    assert len(m.index.get_measurements()) == 1

    # Insert point out of order.
    m.insert(Point(time=test_times["t_year_ago"]))
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


def test_name(db):
    """Test name property of Measurement instance."""
    m = db.measurement("a")
    assert m.name == m._name == "a"


def test_storage(db):
    """Test storage property of Measurement instance."""
    m1 = db.measurement("a")
    m2 = db.measurement("b")
    assert m1.storage == m2.storage == db.storage


def test_all(db, test_times):
    """Test fetching all points from a measurement."""
    p1 = Point(tags={"a": "1"})
    p2 = Point(tags={"a": "2"})
    m = db.measurement("s")

    assert m.all() == []

    m.insert(p1)
    m.insert(p2)
    assert m.all() == [p1, p2]

    p3 = Point(time=test_times["t_past"])
    m.insert(p3)

    # Test "sorted" argument.
    assert m.all(sorted=False) == [p1, p2, p3]
    assert m.all(sorted=True) == [p3, p1, p2]


@pytest.fixture
def query_test_setup(db_no_index):
    """Set up for contains/count/get query tests."""
    db = db_no_index
    m1 = db.measurement("m1")
    m2 = db.measurement("m2")

    # Insert base test data (matching test_contains original data)
    p1 = Point(tags={"a": "A"}, fields={"a": 1})
    p2 = Point(tags={"a": "A"}, fields={"a": 2})
    p3 = Point(tags={"b": "B"}, fields={"b": 1})  # Fixed: was {"a": 1}
    p4 = Point(tags={"b": "B"}, fields={"b": 2})

    m1.insert(p1)
    m1.insert(p2)
    m2.insert(p3)
    m2.insert(p4)

    return db, m1, m2, (p1, p2, p3, p4)


def test_contains(query_test_setup, test_times):
    """Test the contains method of the Measurement class."""
    db, m1, m2, (p1, p2, p3, p4) = query_test_setup

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
            time=test_times["t_year_ago"],
            tags={"a": "c"},
            fields={"a": 3},
        )
    )
    assert not db.index.valid
    assert m1.contains(FieldQuery().a == 3)
    assert not m2.contains(FieldQuery().a == 3)


def test_count(query_test_setup, test_times):
    """Test the count method of the Measurement class."""
    db, m1, m2, (p1, p2, p3, p4) = query_test_setup

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
            time=test_times["t_year_ago"],
            tags={"a": "b", "c": "d"},
            fields={"e": 1, "f": 3},
        )
    )
    assert not db.index.valid
    assert m1.count(FieldQuery().e == 1) == 1


def test_get(query_test_setup, test_times):
    """Test the get method of the Measurement class."""
    db, m1, m2, (p1, p2, p3, p4) = query_test_setup

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
        time=test_times["t_year_ago"],
        tags={"a": "b", "c": "d"},
        fields={"e": 1, "f": 3},
    )
    m1.insert(p5)
    assert not db.index.valid
    assert m1.get(FieldQuery().e == 1) == p5


@pytest.fixture
def measurement_get_setup(db_no_index):
    """Set up for get_*_keys and get_*_values tests."""
    db = db_no_index
    m = db.measurement("_default")
    m2 = db.measurement("some_missing_measurement")
    assert db.index.valid
    return db, m, m2


@pytest.mark.parametrize(
    "method_name,point_data,expected_results",
    [
        (
            "get_field_keys",
            [
                {},  # Empty point
                {"fields": {"a": 1}},  # Single field
                {"fields": {"a": 2, "b": 3}},  # Multiple fields
                {
                    "time": "t_yesterday",
                    "fields": {"a": 1, "c": 3},
                },  # Out of order
            ],
            [
                [],  # After empty point
                ["a"],  # After single field
                ["a", "b"],  # After multiple fields
                ["a", "b", "c"],  # After invalidation
            ],
        ),
        (
            "get_tag_keys",
            [
                {},  # Empty point
                {"tags": {"a": "1"}},  # Single tag
                {"tags": {"a": "2", "b": "3"}},  # Multiple tags
                {
                    "time": "t_yesterday",
                    "tags": {"a": "1", "c": "3"},
                },  # Out of order
            ],
            [
                [],  # After empty point
                ["a"],  # After single tag
                ["a", "b"],  # After multiple tags
                ["a", "b", "c"],  # After invalidation
            ],
        ),
    ],
)
def test_get_keys_methods(
    measurement_get_setup, method_name, point_data, expected_results, test_times
):
    """Test get_field_keys and get_tag_keys methods."""
    db, m, m2 = measurement_get_setup

    # Valid index, nothing in storage/index
    assert getattr(m, method_name)() == []

    for i, (point_spec, expected) in enumerate(
        zip(point_data, expected_results)
    ):
        # Handle time references
        if "time" in point_spec and isinstance(point_spec["time"], str):
            point_spec = {**point_spec, "time": test_times[point_spec["time"]]}

        # Create and insert point
        m.insert(Point(**point_spec))

        # Reindex if not the last item (which invalidates the index)
        if i < len(point_data) - 1:
            db.reindex()

        # Test main measurement
        assert getattr(m, method_name)() == expected

        # Test missing measurement (should always be empty except after
        # invalidation)
        if i < len(point_data) - 1:
            assert getattr(m2, method_name)() == []


@pytest.mark.parametrize(
    "method_name,field_key,point_data,expected_results",
    [
        (
            "get_field_values",
            "a",
            [
                {},  # Empty point
                {"fields": {"a": 1}},  # Single field
                {
                    "fields": {"a": 2, "b": 3}
                },  # Multiple fields with different key
                {
                    "time": "t_yesterday",
                    "fields": {"a": 1, "c": 3},
                },  # Out of order
            ],
            [
                [],  # After empty point
                [1],  # After single field
                [1, 2],  # After multiple fields
                [1, 2, 1],  # After invalidation (includes duplicate)
            ],
        ),
    ],
)
def test_get_field_values(
    measurement_get_setup,
    method_name,
    field_key,
    point_data,
    expected_results,
    test_times,
):
    """Test get_field_values method."""
    db, m, m2 = measurement_get_setup

    # Valid index, nothing in storage/index
    assert getattr(m, method_name)(field_key) == []

    for i, (point_spec, expected) in enumerate(
        zip(point_data, expected_results)
    ):
        # Handle time references
        if "time" in point_spec and isinstance(point_spec["time"], str):
            point_spec = {**point_spec, "time": test_times[point_spec["time"]]}

        # Create and insert point
        m.insert(Point(**point_spec))

        # Reindex if not the last item (which invalidates the index)
        if i < len(point_data) - 1:
            db.reindex()

        # Test main measurement
        assert getattr(m, method_name)(field_key) == expected

        # Test missing measurement (should always be empty)
        if i < len(point_data) - 1:
            assert getattr(m2, method_name)(field_key) == []


def test_get_tag_values(db_no_index, test_times):
    """Test show tag values."""
    db = db_no_index
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
    m.insert(Point(time=test_times["t_yesterday"], tags={"a": "a", "c": "3"}))

    assert m.get_tag_values() == {"a": ["1", "a"], "b": ["2"], "c": ["3"]}
    assert m.get_tag_values(["c"]) == {"c": ["3"]}
    assert m.get_tag_values(["d"]) == {"d": []}
    assert m.get_tag_values(["a", "b"]) == {"a": ["1", "a"], "b": ["2"]}
    assert m.get_tag_values(["c", "d"]) == {"c": ["3"], "d": []}
    assert m2.get_tag_values() == {"a": ["cow", "horse"], "b": ["bird"]}


def test_get_timestamps(db_no_index, test_times):
    """Test get timestamps."""
    db = db_no_index
    m1 = db.measurement("_default")
    m2 = db.measurement("other_measurement")
    assert db.index.valid

    # Valid index, nothing in storage/index.
    assert m1.get_timestamps() == []
    assert m2.get_timestamps() == []

    t1 = test_times["t_now"]
    m1.insert(Point(time=t1))
    db.reindex()
    assert m1.get_timestamps() == [t1]
    assert m2.get_timestamps() == []

    t2 = test_times["t_future"]
    m1.insert(Point(time=t2))
    db.reindex()
    assert m1.get_timestamps() == [t1, t2]
    assert m2.get_timestamps() == []

    t3 = test_times["t_future"]
    m2.insert(Point(time=t3))
    db.reindex()
    assert m1.get_timestamps() == [t1, t2]
    assert m2.get_timestamps() == [t3]

    # Invalidate index.
    t4 = test_times["t_yesterday"]
    db.insert(Point(time=t4))

    assert m1.get_timestamps() == [t1, t2, t4]
    assert m2.get_timestamps() == [t3]


def test_insert(db, test_times):
    """Test the insert method of the Measurement class."""
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

    # Insert in-order, into a different measurement.
    assert m2.insert(p2) == 1
    assert db.index.valid and not db.index.empty
    assert len(db.index) == 2
    assert len(db) == 2
    assert len(m1) == 1
    assert len(m2) == 1

    # Insert out-of-order.  Index should be invalid.
    assert m1.insert(Point(time=test_times["t_yesterday"])) == 1
    assert not db.index.valid
    assert len(db.index) == 0
    assert len(m1) == 2
    assert len(m2) == 1
    assert len(db) == 3


def test_insert_multiple(db):
    """Test the insert multiple method of the Measurement class."""
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


def test_remove_all(db, test_times):
    """Test the remove all method of the Measurement class."""
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
            time=test_times["t_year_ago"],
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
    with pytest.raises(ValueError):
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


def test_insert_multiple_batch_size_in_order(
    batch_measurement, db_with_index, ordered_points
):
    """Test insert_multiple with batch_size=2, points in time order.

    Index should stay valid when points are inserted in order.
    """
    # Insert with batch_size=2 (should create 2 batches)
    count = batch_measurement.insert_multiple(ordered_points, batch_size=2)

    # Verify results
    assert count == 4
    assert len(batch_measurement) == 4
    assert (
        db_with_index.index.valid
    )  # Index should remain valid (points in order)
    assert len(db_with_index.index) == 4  # All points in index

    # Verify data integrity with queries
    high_value_points = batch_measurement.search(FieldQuery().value >= 2)
    assert len(high_value_points) == 2  # Points with value 2 and 3


def test_insert_multiple_batch_size_out_of_order(
    batch_measurement, db_with_index, out_of_order_points
):
    """Test insert_multiple with batch_size=2, points out of time order.

    Index should be invalidated when points are out of order.
    """
    # Insert with batch_size=2 (should create 2 batches)
    count = batch_measurement.insert_multiple(out_of_order_points, batch_size=2)

    # Verify results
    assert count == 4
    assert len(batch_measurement) == 4
    assert (
        not db_with_index.index.valid
    )  # Index should be invalidated (points out of order)

    # Data should still be retrievable by reading storage directly
    all_points = batch_measurement.all()
    assert len(all_points) == 4


def test_insert_multiple_batch_size_3_in_order(
    batch_measurement, db_with_index, base_time
):
    """Test insert_multiple with batch_size=3, points in time order."""
    # Create 6 points in time order
    points = []
    for i in range(6):
        point = Point(
            time=base_time + timedelta(seconds=i * 5),
            measurement="test_batch",
            tags={"sensor": f"sensor_{i % 3}"},
            fields={"temperature": 20.0 + i, "humidity": 50.0 + i},
        )
        points.append(point)

    # Insert with batch_size=3 (should create 2 batches)
    count = batch_measurement.insert_multiple(points, batch_size=3)

    # Verify results
    assert count == 6
    assert len(batch_measurement) == 6
    assert (
        db_with_index.index.valid
    )  # Index should remain valid (points in order)
    assert len(db_with_index.index) == 6  # All points in index

    # Test complex queries work correctly
    high_temp_points = batch_measurement.search(FieldQuery().temperature > 23.0)
    assert len(high_temp_points) == 2  # Points with temp 24.0 and 25.0

    sensor_0_points = batch_measurement.search(TagQuery().sensor == "sensor_0")
    assert len(sensor_0_points) == 2  # Points 0 and 3


def test_insert_multiple_batch_size_3_out_of_order(
    batch_measurement, db_with_index, base_time
):
    """Test insert_multiple with batch_size=3, points out of time order."""
    # Create 6 points with one out of order in second batch
    points = [
        Point(
            time=base_time + timedelta(seconds=0),
            measurement="test_batch",
            tags={"id": "p0"},
            fields={"seq": 0},
        ),
        Point(
            time=base_time + timedelta(seconds=10),
            measurement="test_batch",
            tags={"id": "p1"},
            fields={"seq": 1},
        ),
        Point(
            time=base_time + timedelta(seconds=20),
            measurement="test_batch",
            tags={"id": "p2"},
            fields={"seq": 2},
        ),
        # Second batch - first point in order, second out of order
        Point(
            time=base_time + timedelta(seconds=30),
            measurement="test_batch",
            tags={"id": "p3"},
            fields={"seq": 3},
        ),
        Point(
            time=base_time + timedelta(seconds=15),
            measurement="test_batch",  # Out of order!
            tags={"id": "p4"},
            fields={"seq": 4},
        ),
        Point(
            time=base_time + timedelta(seconds=40),
            measurement="test_batch",
            tags={"id": "p5"},
            fields={"seq": 5},
        ),
    ]

    # Insert with batch_size=3 (should create 2 batches)
    count = batch_measurement.insert_multiple(points, batch_size=3)

    # Verify results
    assert count == 6
    assert len(batch_measurement) == 6
    assert (
        not db_with_index.index.valid
    )  # Index should be invalidated (points out of order)

    # Data should still be accessible
    all_points = batch_measurement.all()
    assert len(all_points) == 6


def test_insert_multiple_batch_size_edge_cases(
    batch_measurement, db_with_index, base_time
):
    """Test insert_multiple with edge cases."""
    # Test empty list
    count = batch_measurement.insert_multiple([], batch_size=2)
    assert count == 0
    assert len(batch_measurement) == 0
    assert db_with_index.index.valid  # Index should remain valid

    # Test single point
    single_point = [
        Point(
            time=base_time,
            measurement="test_batch",
            tags={"single": "true"},
            fields={"value": 42.0},
        )
    ]

    count = batch_measurement.insert_multiple(single_point, batch_size=2)
    assert count == 1
    assert len(batch_measurement) == 1
    assert db_with_index.index.valid  # Index should remain valid
    assert len(db_with_index.index) == 1

    # Test batch_size larger than point count
    more_points = [
        Point(
            time=base_time + timedelta(seconds=10),
            measurement="test_batch",
            tags={"more": "p1"},
            fields={"value": 1.0},
        ),
        Point(
            time=base_time + timedelta(seconds=20),
            measurement="test_batch",
            tags={"more": "p2"},
            fields={"value": 2.0},
        ),
    ]

    count = batch_measurement.insert_multiple(
        more_points, batch_size=10
    )  # Batch larger than data
    assert count == 2
    assert len(batch_measurement) == 3  # 1 from before + 2 new
    assert db_with_index.index.valid  # Index should remain valid
    assert len(db_with_index.index) == 3


def test_insert_multiple_auto_index_false(db_no_index, base_time):
    """Test insert_multiple with auto_index=False."""
    measurement = db_no_index.measurement("test_batch")

    points = [
        Point(
            time=base_time,
            measurement="test_batch",
            tags={"test": "1"},
            fields={"value": 1.0},
        ),
        Point(
            time=base_time + timedelta(seconds=10),
            measurement="test_batch",
            tags={"test": "2"},
            fields={"value": 2.0},
        ),
    ]

    # Insert with batch_size=2
    count = measurement.insert_multiple(points, batch_size=2)

    # Verify results
    assert count == 2
    assert len(measurement) == 2
    assert (
        not db_no_index.index.valid
    )  # Index should be invalid (auto_index=False)

    # Data should still be accessible through storage
    all_points = measurement.all()
    assert len(all_points) == 2


def test_insert_multiple_batch_size_validation_measurement(
    batch_measurement, ordered_points
):
    """Test insert_multiple batch_size validation on Measurement."""
    # Test batch_size = 0
    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        batch_measurement.insert_multiple(ordered_points, batch_size=0)

    # Test batch_size < 0
    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        batch_measurement.insert_multiple(ordered_points, batch_size=-1)

    # Test batch_size = 1 (should work)
    count = batch_measurement.insert_multiple(ordered_points, batch_size=1)
    assert count == 4


def test_insert_multiple_batch_size_validation_database(
    db_with_index, ordered_points
):
    """Test insert_multiple batch_size validation on TinyFlux database."""
    # Test batch_size = 0
    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        db_with_index.insert_multiple(
            ordered_points, measurement="test_batch", batch_size=0
        )

    # Test batch_size < 0
    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        db_with_index.insert_multiple(
            ordered_points, measurement="test_batch", batch_size=-1
        )

    # Test batch_size = 1 (should work)
    count = db_with_index.insert_multiple(
        ordered_points, measurement="test_batch", batch_size=1
    )
    assert count == 4
