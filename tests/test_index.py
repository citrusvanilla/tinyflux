"""Tests for tinyflux.index module."""
from datetime import datetime, timezone, timedelta
import pytest

from tinyflux import Point, FieldQuery, TagQuery, MeasurementQuery, TimeQuery
from tinyflux.index import Index


def test_repr():
    """Test __repr__ of Index."""
    t = datetime.now(timezone.utc)
    index = Index()

    assert repr(index) == "<Index _tags=0, _measurements=0, _timestamps=0>"

    index = Index()
    index.build([Point(tags={"tk": "tv"}), Point(time=t), Point(time=t)])

    assert repr(index) == "<Index _tags=1, _measurements=1, _timestamps=3>"


def test_initialize_empty_index():
    """Test initializing an empty Index."""
    index = Index()

    assert isinstance(index._num_items, int)
    assert not index._num_items

    assert isinstance(index._tags, dict)
    assert not index._tags

    assert isinstance(index._fields, dict)
    assert not index._fields

    assert isinstance(index._measurements, dict)
    assert not index._measurements

    assert isinstance(index._timestamps, list)
    assert not index._timestamps


def test_build():
    """Test building an Index."""
    t1 = datetime.now(timezone.utc)
    t2 = datetime.now(timezone.utc) + timedelta(seconds=1)
    t3 = datetime.now(timezone.utc) + timedelta(seconds=2)

    p1 = Point(time=t1, tags={"city": "la"})
    p2 = Point(
        time=t2, measurement="cities", tags={"city": "sf"}, fields={"temp": 70}
    )
    p3 = Point(
        time=t3,
        measurement="states",
        tags={"state": "ca"},
        fields={"pop": 30000000},
    )

    index = Index()
    assert index.empty

    index.build([])
    assert index.empty

    index.build([Point(time=t1), Point(time=t1)])
    assert index._num_items == 2

    index.build([Point(time=t1)])
    assert index._num_items == 1

    index = Index()
    index.build([p1, p2, p3])

    assert index._num_items == 3

    assert index._tags == {
        "city": {"la": [0], "sf": [1]},
        "state": {"ca": [2]},
    }

    assert index._fields == {"temp": [(1, 70)], "pop": [(2, 30000000)]}

    assert index._measurements == {
        "_default": [0],
        "cities": [1],
        "states": [2],
    }

    assert index._timestamps == [i.timestamp() for i in [t1, t2, t3]]


def test_empty_property():
    """Test is_empty property of Index."""
    index = Index()
    t = datetime.now(timezone.utc)
    assert index.empty

    index.build([Point(time=t) for _ in range(10)])
    assert not index.empty

    index._reset()
    assert index.empty

    index.insert([Point(time=t) for _ in range(10)])
    assert not index.empty


def test_insert_time_method():
    """Test _insert_time helper of Index."""
    index = Index()

    t1 = datetime.now(timezone.utc)
    index._insert_time(t1)
    assert index._timestamps == [t1.timestamp()]

    t2 = datetime.now(timezone.utc)
    index._insert_time(t2)
    assert index._timestamps == [t1.timestamp(), t2.timestamp()]


def test_index_measuments_method():
    """Test _insert_measurements helper of Index."""
    index = Index()

    index._insert_measurements(0, "_default")
    assert index._measurements == {"_default": [0]}

    index._insert_measurements(1, "cities")
    assert index._measurements == {"_default": [0], "cities": [1]}


def test_insert_tags_method():
    """Test _insert_tags helper of Index."""
    index = Index()

    index._insert_tags(0, {"city": "la"})
    assert index._tags == {"city": {"la": [0]}}

    index._insert_tags(1, {"state": "ca"})
    assert index._tags == {"city": {"la": [0]}, "state": {"ca": [1]}}

    index._insert_tags(2, {"city": "la"})
    assert index._tags == {"city": {"la": [0, 2]}, "state": {"ca": [1]}}


def test_insert_fields_method():
    """Test _insert_fields helper of Index."""
    index = Index()

    index._insert_fields(0, {"temp": 70.0})
    assert index._fields == {"temp": [(0, 70.0)]}

    index._insert_fields(1, {"temp": 71.0})
    assert index._fields == {"temp": [(0, 70.0), (1, 71.0)]}

    index._insert_fields(2, {"pop": 5000})
    assert index._fields == {
        "temp": [(0, 70.0), (1, 71.0)],
        "pop": [(2, 5000)],
    }


def test_reset_method():
    """Test reset of Index."""
    index = Index()
    index.insert([Point(time=datetime.now(timezone.utc))])
    assert not index.empty

    index._reset()
    assert index.empty


def test_search_helper_exception():
    """Test that the search helper of the index raises exceptions."""
    index = Index()

    with pytest.raises(
        TypeError, match="Query must be SimpleQuery or CompoundQuery."
    ):
        index._search_helper(TimeQuery())

    with pytest.raises(
        TypeError, match="Query must be SimpleQuery or CompoundQuery."
    ):
        index._search_helper(TagQuery().a)

    with pytest.raises(
        TypeError, match="Query must be SimpleQuery or CompoundQuery."
    ):
        index._search_helper(FieldQuery().a)


def test_search_measurement_query():
    """Test search_query of Index on MeasurementQuery."""
    index = Index()
    q = MeasurementQuery() == "_default"

    index._insert_measurements(0, "_default")
    index._insert_measurements(1, "cities")
    index._insert_measurements(2, "_default")
    assert index._measurements == {"_default": [0, 2], "cities": [1]}

    rst = index.search(q)
    assert rst.items == {0, 2}


def test_search_time_query():
    """Test search_query of Index on TimeQuery."""
    index = Index()
    t_now = datetime.now(timezone.utc)

    t0 = t_now - timedelta(days=3)
    t1 = t_now - timedelta(days=2)
    t2 = t_now - timedelta(days=1)
    t3 = t_now
    t4 = t_now
    t5 = t_now + timedelta(days=1)
    t6 = t_now + timedelta(days=2)

    index._insert_time(t1)
    index._insert_time(t2)
    index._insert_time(t3)
    index._insert_time(t4)
    index._insert_time(t5)
    assert index._timestamps == [i.timestamp() for i in [t1, t2, t3, t4, t5]]

    # Less than or equal.
    q = TimeQuery() <= t0
    assert index.search(q).items == set({})
    q = TimeQuery() <= t1
    assert index.search(q).items == {0}
    q = TimeQuery() <= t4
    assert index.search(q).items == {0, 1, 2, 3}

    # Less than.
    q = TimeQuery() < t1
    assert index.search(q).items == set({})
    q = TimeQuery() < t3
    assert index.search(q).items == {0, 1}

    # Greater than or equal.
    q = TimeQuery() >= t1
    assert index.search(q).items == {0, 1, 2, 3, 4}
    q = TimeQuery() >= t3
    assert index.search(q).items == {2, 3, 4}
    q = TimeQuery() >= t6
    assert index.search(q).items == set({})

    # Greater than.
    q = TimeQuery() > t2
    assert index.search(q).items == {2, 3, 4}
    q = TimeQuery() > t5
    assert index.search(q).items == set({})

    # Equal to.
    q = TimeQuery() == t1
    assert index.search(q).items == {0}
    q = TimeQuery() == t3
    assert index.search(q).items == {2, 3}
    q = TimeQuery() == t6
    assert index.search(q).items == set({})

    # Not equal to.
    q = TimeQuery() != t2
    assert index.search(q).items == {0, 2, 3, 4}
    q = TimeQuery() != t3
    assert index.search(q).items == {0, 1, 4}
    q = TimeQuery() != t6
    assert index.search(q).items == {0, 1, 2, 3, 4}

    # Other type of test.
    q = TimeQuery().test(lambda x: x != t2)
    assert index.search(q).items == {0, 2, 3, 4}
    q = TimeQuery().test(lambda x: x != t3)
    assert index.search(q).items == {0, 1, 4}
    q = TimeQuery().test(lambda x: x != t6)
    assert index.search(q).items == {0, 1, 2, 3, 4}


def test_search_tags_query():
    """Test search_query of Index on TagQuery."""
    index = Index()

    index._insert_tags(0, {"city": "la", "state": "ca"})
    index._insert_tags(1, {"city": "sf", "state": "ca"})
    index._insert_tags(2, {"city": "sf"})
    index._insert_tags(3, {"neighborhood": "dtla"})
    assert index._tags == {
        "city": {"la": [0], "sf": [1, 2]},
        "state": {"ca": [0, 1]},
        "neighborhood": {"dtla": [3]},
    }

    rst = index.search(TagQuery().city == "la")
    assert rst.items == {0}

    rst = index.search(TagQuery().city != "la")
    assert rst.items == {1, 2}

    rst = index.search(TagQuery().city == "sf")
    assert rst.items == {1, 2}

    rst = index.search(TagQuery().city != "sf")
    assert rst.items == {0}

    rst = index.search(TagQuery().state == "ca")
    assert rst.items == {0, 1}

    rst = index.search(TagQuery().state != "ca")
    assert rst.items == set({})

    rst = index.search(TagQuery().neighborhood == "dtla")
    assert rst.items == {3}

    rst = index.search(TagQuery().neighborhood != "dtla")
    assert rst.items == set({})


def test_search_field_query():
    """Test search_query of Index on FieldQuery."""
    # An index.
    index = Index()

    index._insert_fields(0, {"temp": 78.3})
    index._insert_fields(1, {"temp": 59.1})
    index._insert_fields(2, {"pop": 30000000})
    assert index._fields == {
        "temp": [(0, 78.3), (1, 59.1)],
        "pop": [(2, 30000000)],
    }

    # Queries.
    rst = index.search(FieldQuery().temp == 70.0)
    assert rst.items == set({})

    rst = index.search(FieldQuery().temp != 70.0)
    assert rst.items == {0, 1}

    rst = index.search(FieldQuery().pop >= 10000000)
    assert rst.items == set({2})

    rst = index.search(FieldQuery().pop > 40000000)
    assert rst.items == set({})

    rst = index.search(FieldQuery().pop < 1000)
    assert rst.items == set({})

    rst = index.search(FieldQuery().pop <= 1000)
    assert rst.items == set({})


def test_search_compound_query_not():
    """Test search_query of Index on compound 'not' queries."""
    # Some timestamps.
    t_now = datetime.now(timezone.utc)

    # Some points.
    p1 = Point(
        time=t_now - timedelta(days=1),
        tags={"city": "la"},
        fields={"temp": 70.0},
    )
    p2 = Point(time=t_now, tags={"state": "ca"}, fields={"pop": 30000000})

    # An index.
    index = Index()
    index.build([p1, p2])

    # Query types.
    meas_q = MeasurementQuery() == "cities"
    fiel_q = FieldQuery().temp == 70.0
    time_q = TimeQuery() == t_now
    tags_q = TagQuery().city == "la"

    # Measurement query.
    rst = index.search(~meas_q)
    assert rst.items == {0, 1}

    # Field query. Note for Field Queries, a NOT operator means we have to
    # check every single item in the storage layer.
    rst = index.search(~fiel_q)
    assert rst.items == {0, 1}

    # Compount NOT FieldQuery.
    rst = index.search(~fiel_q & tags_q)
    assert rst.items == {0}

    # Time query.
    rst = index.search(~time_q)
    assert rst.items == {0}

    # Tag query.
    rst = index.search(~tags_q)
    assert rst.items == {1}


def test_search_compound_query_and():
    """Test search_query of Index on compound 'and' queries."""
    # Some timestamps.
    t_now = datetime.now(timezone.utc)

    # Some points.
    p1 = Point(
        time=t_now - timedelta(days=2),
        measurement="cities",
        tags={"city": "la"},
        fields={"temp": 70.0},
    )
    p2 = Point(
        time=t_now - timedelta(days=1),
        measurement="states",
        tags={"state": "ca"},
        fields={"pop": 30000000},
    )
    p3 = Point(
        time=t_now,
        measurement="cities",
        tags={"city": "la"},
        fields={"temp": 82.8},
    )

    # An index.
    index = Index()
    index.build([p1, p2, p3])

    # Query types.
    meas_q = MeasurementQuery() == "cities"
    fiel_q = FieldQuery().temp == 70.0
    time_q = TimeQuery() == t_now
    tags_q = TagQuery().city == "la"

    # Measurement and Field.
    rst = index.search(meas_q & fiel_q)
    assert rst.items == {0}

    # Measurement and Time.
    rst = index.search(meas_q & time_q)
    assert rst.items == {2}

    # Measurement and Tags.
    rst = index.search(meas_q & tags_q)
    assert rst.items == {0, 2}

    # Field and Time.
    rst = index.search(fiel_q & time_q)
    assert rst.items == set({})

    # Field and Tags.
    rst = index.search(fiel_q & tags_q)
    assert rst.items == {0}

    # Time and Tags.
    rst = index.search(time_q & tags_q)
    assert rst.items == {2}


def test_search_compound_query_or():
    """Test search_query of Index on compound 'or' queries."""
    # Some timestamps.
    t_now = datetime.now(timezone.utc)

    # Some points.
    p1 = Point(
        time=t_now - timedelta(days=2),
        measurement="cities",
        tags={"city": "la"},
        fields={"temp": 70.0},
    )
    p2 = Point(
        time=t_now - timedelta(days=1),
        measurement="states",
        tags={"state": "ca"},
        fields={"pop": 30000000},
    )
    p3 = Point(
        time=t_now,
        measurement="cities",
        tags={"city": "la"},
        fields={"temp": 82.8},
    )

    # An index.
    index = Index()
    index.build([p1, p2, p3])

    # Query types.
    meas_q = MeasurementQuery() == "cities"
    fiel_q = FieldQuery().temp == 70.0
    time_q = TimeQuery() == t_now
    tags_q = TagQuery().city == "la"

    # Measurement or Field.
    rst = index.search(meas_q | fiel_q)
    assert rst.items == {0, 2}

    # Measurement or Time.
    rst = index.search(meas_q | time_q)
    assert rst.items == {0, 2}

    # Measurement or Tags.
    rst = index.search(meas_q | tags_q)
    assert rst.items == {0, 2}

    # Field or Time.
    rst = index.search(fiel_q | time_q)
    assert rst.items == {0, 2}

    # Field or Tags.
    rst = index.search(fiel_q | tags_q)
    assert rst.items == {0, 2}

    # Time or Tags.
    rst = index.search(time_q | tags_q)
    assert rst.items == {0, 2}


def test_update():
    """Test update method of Index."""
    index = Index()
    t = datetime.now(timezone.utc)

    index.insert([Point(time=t), Point(time=t)])
    assert index._num_items == 2

    index.insert([Point(time=t)])
    assert index._num_items == 3
