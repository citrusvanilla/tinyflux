"""Tests for the tinyflux.queries module."""
import operator
from datetime import datetime, timezone, timedelta
from itertools import combinations

import pytest

from tinyflux import Point
from tinyflux.queries import (
    CompoundQuery,
    BaseQuery,
    TagQuery,
    FieldQuery,
    MeasurementQuery,
    TimeQuery,
    SimpleQuery,
)


def test_init():
    """Test initialization of base queries."""
    for q in [TimeQuery(), MeasurementQuery(), TagQuery(), FieldQuery()]:
        assert isinstance(q, q.__class__)
        assert isinstance(q, BaseQuery)
        assert not isinstance(q, SimpleQuery)
        assert not isinstance(q, CompoundQuery)

        with pytest.raises(TypeError):
            q()


def test_repr():
    """Test __repr__ of a query."""
    # SimpleQuery
    t1 = datetime.now(timezone.utc)

    q1 = TagQuery().city == "los angeles"
    q2 = FieldQuery().temperature <= 70.0
    q3 = MeasurementQuery() == "some measurement"
    q4 = TimeQuery() != t1

    r1 = "SimpleQuery('_tags', '==', ('city',), 'los angeles')"
    r2 = "SimpleQuery('_fields', '<=', ('temperature',), 70.0)"
    r3 = "SimpleQuery('_measurement', '==', (), 'some measurement')"
    r4 = f"SimpleQuery('_time', '!=', (), {repr(t1)})"

    assert repr(q1) == r1
    assert repr(q2) == r2
    assert repr(q3) == r3
    assert repr(q4) == r4
    assert repr(BaseQuery()) == "BaseQuery()"
    assert repr(TagQuery().a.map(lambda x: x) == "b") == "SimpleQuery()"

    # CompoundQuery
    c1 = q1 & q2
    c2 = q1 | q3
    c3 = ~q1
    c4 = q2 & q3
    c5 = q2 | q4
    c6 = ~q3

    rc1 = (
        f"CompoundQuery({c1.operator.__name__}, 'SimpleQuery', 'SimpleQuery')"
    )
    rc2 = (
        f"CompoundQuery({c2.operator.__name__}, 'SimpleQuery', 'SimpleQuery')"
    )
    rc3 = f"CompoundQuery({c3.operator.__name__}, 'SimpleQuery')"
    rc4 = (
        f"CompoundQuery({c4.operator.__name__}, 'SimpleQuery', 'SimpleQuery')"
    )
    rc5 = (
        f"CompoundQuery({c5.operator.__name__}, 'SimpleQuery', 'SimpleQuery')"
    )
    rc6 = "CompoundQuery(not_, 'SimpleQuery')"

    assert repr(c1) == rc1
    assert repr(c2) == rc2
    assert repr(c3) == rc3
    assert repr(c4) == rc4
    assert repr(c5) == rc5
    assert repr(c6) == rc6


def test_no_path():
    """Test absense of path for Tag and Field queries.

    TagQuery and FieldQuery should have paths.
    """
    for q in (TagQuery, FieldQuery):
        with pytest.raises(RuntimeError):
            _ = q() == "some_value"


def test_path_exists():
    """Test the "exists" attribute of a Tag and Field Query.

    Tests for the existence of the key on any point.
    """
    # Test for existence of the tag key "city".
    taq_q = TagQuery().city.exists()
    assert taq_q(Point(tags={"city": "los angeles"}))
    assert not taq_q(Point())
    assert not taq_q(Point(tags={"state": "california"}))
    assert hash(taq_q)

    # Test for existence of the field key "temperature".
    field_q = FieldQuery().temperature.exists()
    assert field_q(Point(fields={"temperature": 70.0}))
    assert not field_q(Point())
    assert not field_q(Point(fields={"precipitation": 0.25}))
    assert hash(field_q)


def test_path_and():
    """Test a compound query with a path.

    Tests for both Tag and Field queries.
    """
    # Test tagquery path, and general tag query.
    q1, q2 = TagQuery().city.exists(), TagQuery().city == "los angeles"
    compound_q = q1 & q2

    assert compound_q(Point(tags={"city": "los angeles"}))
    assert not compound_q(Point())
    assert not compound_q(Point(tags={"city": "san francisco"}))
    assert not compound_q(Point(tags={"state": "california"}))
    assert hash(compound_q)

    # Test field query path, and general field query.
    q1, q2 = (
        FieldQuery().temperature.exists(),
        FieldQuery().temperature == 70.0,
    )
    compound_q = q1 & q2

    assert compound_q(Point(fields={"temperature": 70.0}))
    assert not compound_q(Point())
    assert not compound_q(Point(fields={"temperature": 71.0}))
    assert not compound_q(Point(fields={"precipitation": 0.25}))
    assert hash(compound_q)


def test_callable_in_path_with_map():
    """Test arbitrary callable in a query."""
    # Test arbitrary map function.
    q1 = FieldQuery().a.map(lambda x: x * 2) == 140.0
    q2 = FieldQuery().a.map(lambda x: x % 10 == 0) == True  # noqa: E712
    assert q1(Point(fields={"a": 70.0}))
    assert not q1(Point(fields={"a": 69.0}))
    assert not q1(Point(fields={"b": 5000}))
    assert (q1 & q2)(Point(fields={"a": 70.0}))


def test_callable_in_path_with_chain():
    """Test a callable in a path."""

    def rekey(x):
        """Rekey test function."""
        return {"y": x["a"], "z": x["b"]}

    query = TagQuery().map(rekey).z == "some val"
    assert query(Point(tags={"a": "a val", "b": "some val"}))


def test_eq():
    """Test simple equality."""
    taq_q = TagQuery().city == "a"
    assert not taq_q(Point(tags={"city": "los angeles"}))

    # Test for tag key "city".
    taq_q = TagQuery().city == "los angeles"
    assert taq_q(Point(tags={"city": "los angeles"}))
    assert not taq_q(Point())
    assert not taq_q(Point(tags={"state": "california"}))
    assert hash(taq_q)

    # Test for field key "temperature".
    field_q = FieldQuery().temperature == 70.0
    assert field_q(Point(fields={"temperature": 70.0}))
    assert not field_q(Point())
    assert not field_q(Point(fields={"precipitation": 0.25}))
    assert hash(field_q)

    # Test for measurement.
    measurement_q = MeasurementQuery() == "some measurement"
    assert measurement_q(Point(measurement="some measurement"))
    assert not measurement_q(Point())
    assert not measurement_q(Point(measurement="some other measurement"))
    assert hash(measurement_q)

    # Test for time.
    time_now = datetime.now(timezone.utc)
    time_q = TimeQuery() == time_now
    assert time_q(Point(time=time_now))
    assert not time_q(Point(time=time_now - timedelta(days=1)))
    assert hash(time_q)


def test_ne():
    """Test simple inequality."""
    # Test for tag key "city".
    taq_q = TagQuery().city != "los angeles"
    assert taq_q(Point(tags={"city": "san francisco"}))
    assert not taq_q(Point(tags={"city": "los angeles"}))
    assert not taq_q(Point())
    assert hash(taq_q)

    # Test for field key "temperature".
    field_q = FieldQuery().temperature != 70.0
    assert field_q(Point(fields={"temperature": 69.0}))
    assert not field_q(Point(fields={"temperature": 70.0}))
    assert not field_q(Point())
    assert not field_q(Point(fields={"precipitation": 0.25}))
    assert hash(field_q)

    # Test for measurement.
    measurement_q = MeasurementQuery() != "some measurement"
    assert measurement_q(Point(measurement="some other measurement"))
    assert not measurement_q(Point(measurement="some measurement"))
    assert measurement_q(Point())
    assert hash(measurement_q)

    # Test for time.
    time_now = datetime.now(timezone.utc)
    time_q = TimeQuery() != time_now
    assert time_q(Point(time=time_now - timedelta(days=1)))
    assert not time_q(Point(time=time_now))
    assert hash(time_q)


def test_lt():
    """Test simple less than comparison."""
    # Test for tag key "city".
    taq_q = TagQuery().city < "melbourne"
    assert taq_q(Point(tags={"city": "amsterdam"}))
    assert not taq_q(Point(tags={"city": "zansibar"}))
    assert not taq_q(Point())
    assert hash(taq_q)

    # Test for field key "temperature".
    field_q = FieldQuery().temperature < 70.0
    assert field_q(Point(fields={"temperature": 69.0}))
    assert not field_q(Point(fields={"temperature": 70.0}))
    assert not field_q(Point())
    assert not field_q(Point(fields={"precipitation": 0.25}))
    assert hash(field_q)

    # Test for measurement.
    measurement_q = MeasurementQuery() < "my measurement"
    assert measurement_q(Point(measurement="a measurement"))
    assert not measurement_q(Point(measurement="some measurement"))
    assert measurement_q(Point())
    assert hash(measurement_q)

    # Test for time.
    time_now = datetime.now(timezone.utc)
    time_q = TimeQuery() < time_now
    assert time_q(Point(time=time_now - timedelta(days=1)))
    assert not time_q(Point(time=time_now))
    assert hash(time_q)


def test_le():
    """Test simple less than or equal to comparison."""
    # Test for tag key "city".
    taq_q = TagQuery().city <= "melbourne"
    assert taq_q(Point(tags={"city": "melbourne"}))
    assert not taq_q(Point(tags={"city": "zansibar"}))
    assert not taq_q(Point())
    assert hash(taq_q)

    # Test for field key "temperature".
    field_q = FieldQuery().temperature <= 70.0
    assert field_q(Point(fields={"temperature": 70.0}))
    assert not field_q(Point(fields={"temperature": 71.0}))
    assert not field_q(Point())
    assert not field_q(Point(fields={"precipitation": 0.25}))
    assert hash(field_q)

    # Test for measurement.
    measurement_q = MeasurementQuery() <= "my measurement"
    assert measurement_q(Point(measurement="my measurement"))
    assert not measurement_q(Point(measurement="some measurement"))
    assert measurement_q(Point())
    assert hash(measurement_q)

    # Test for time.
    time_now = datetime.now(timezone.utc)
    time_q = TimeQuery() <= time_now
    assert time_q(Point(time=time_now))
    assert not time_q(Point(time=time_now + timedelta(days=1)))
    assert hash(time_q)


def test_gt():
    """Test simple greater than comparison."""
    # Test for tag key "city".
    taq_q = TagQuery().city > "melbourne"
    assert taq_q(Point(tags={"city": "zansibar"}))
    assert not taq_q(Point(tags={"city": "amsterdam"}))
    assert not taq_q(Point())
    assert hash(taq_q)

    # Test for field key "temperature".
    field_q = FieldQuery().temperature > 70.0
    assert field_q(Point(fields={"temperature": 71.0}))
    assert not field_q(Point(fields={"temperature": 70.0}))
    assert not field_q(Point())
    assert not field_q(Point(fields={"precipitation": 0.25}))
    assert hash(field_q)

    # Test for measurement.
    measurement_q = MeasurementQuery() > "my measurement"
    assert measurement_q(Point(measurement="some measurement"))
    assert not measurement_q(Point(measurement="a measurement"))
    assert not measurement_q(Point())
    assert hash(measurement_q)

    # Test for time.
    time_now = datetime.now(timezone.utc)
    time_q = TimeQuery() > time_now
    assert time_q(Point(time=time_now + timedelta(days=1)))
    assert not time_q(Point(time=time_now))
    assert hash(time_q)


def test_ge():
    """Test simple greater than or equal to comparison."""
    # Test for tag key "city".
    taq_q = TagQuery().city >= "melbourne"
    assert taq_q(Point(tags={"city": "melbourne"}))
    assert not taq_q(Point(tags={"city": "amsterdam"}))
    assert not taq_q(Point())
    assert hash(taq_q)

    # Test for field key "temperature".
    field_q = FieldQuery().temperature >= 70.0
    assert field_q(Point(fields={"temperature": 70.0}))
    assert not field_q(Point(fields={"temperature": 69.0}))
    assert not field_q(Point())
    assert not field_q(Point(fields={"precipitation": 0.25}))
    assert hash(field_q)

    # Test for measurement.
    measurement_q = MeasurementQuery() >= "my measurement"
    assert measurement_q(Point(measurement="my measurement"))
    assert not measurement_q(Point(measurement="a measurement"))
    assert not measurement_q(Point())
    assert hash(measurement_q)

    # Test for time.
    time_now = datetime.now(timezone.utc)
    time_q = TimeQuery() >= time_now
    assert time_q(Point(time=time_now))
    assert not time_q(Point(time=time_now - timedelta(days=1)))
    assert hash(time_q)


def test_or():
    """Test logical OR."""
    # Test tag query.
    q1, q2 = TagQuery().city == "los angeles", TagQuery().state == "california"
    q3 = q1 | q2
    assert q3(Point(tags={"city": "los angeles"}))
    assert q3(Point(tags={"state": "california"}))
    assert q3(Point(tags={"city": "los angeles", "state": "california"}))
    assert not q3(Point(tags={"city": "phoenix"}))
    assert not q3(Point(tags={"state": "arizona"}))
    assert not q3(Point(tags={"city": "phoenix", "state": "arizona"}))
    assert not q3(Point())
    assert hash(q3)

    # Test field query.
    q1, q2 = FieldQuery().temperature < 32.0, FieldQuery().precipitation > 0.0
    q3 = q1 | q2
    assert q3(Point(fields={"temperature": 25.0}))
    assert q3(Point(fields={"precipitation": 0.25}))
    assert q3(Point(fields={"temperature": 25.0, "precipitation": 0.0}))
    assert not q3(Point(fields={"temperature": 70.0}))
    assert not q3(Point(fields={"precipitation": 0.0}))
    assert not q3(Point(fields={"temperature": 70.0, "precipitation": 0.0}))
    assert not q3(Point())
    assert hash(q3)

    # Test measurement query.
    q1, q2 = (
        MeasurementQuery() == "cities",
        MeasurementQuery() == "weather stations",
    )
    q3 = q1 | q2
    assert q3(Point(measurement="cities"))
    assert q3(Point(measurement="weather stations"))
    assert not q3(Point(measurement="states"))
    assert not q3(Point(measurement="taco truck count"))
    assert not q3(Point())
    assert hash(q3)

    # Test time query.
    t1 = datetime.now(timezone.utc) - timedelta(days=365 * 1)
    t2 = datetime.now(timezone.utc) + timedelta(days=365 * 1)
    q1, q2 = TimeQuery() <= t1, TimeQuery() >= t2
    q3 = q1 | q2
    assert q3(Point(time=t1))
    assert q3(Point(time=t2))
    assert not q3(Point())
    assert not q3(Point(time=t1 + timedelta(days=365 * 1)))
    assert hash(q3)


def test_and():
    """Test logical OR."""
    # Test tag query.
    q1, q2 = TagQuery().city == "los angeles", TagQuery().state == "california"
    q3 = q1 & q2
    assert not q3(Point(tags={"city": "los angeles"}))
    assert not q3(Point(tags={"state": "california"}))
    assert q3(Point(tags={"city": "los angeles", "state": "california"}))
    assert not q3(Point(tags={"city": "phoenix"}))
    assert not q3(Point(tags={"state": "arizona"}))
    assert not q3(Point(tags={"city": "phoenix", "state": "arizona"}))
    assert not q3(Point())
    assert hash(q3)

    # Test field query.
    q1, q2 = FieldQuery().temperature < 32.0, FieldQuery().precipitation > 0.0
    q3 = q1 & q2
    assert not q3(Point(fields={"temperature": 25.0}))
    assert not q3(Point(fields={"precipitation": 0.25}))
    assert q3(Point(fields={"temperature": 25.0, "precipitation": 0.25}))
    assert not q3(Point(fields={"temperature": 70.0}))
    assert not q3(Point(fields={"precipitation": 0.0}))
    assert not q3(Point(fields={"temperature": 70.0, "precipitation": 0.0}))
    assert not q3(Point())
    assert hash(q3)

    # Test measurement query.
    q1, q2 = (
        MeasurementQuery() == "cities",
        MeasurementQuery() == "weather stations",
    )
    q3 = q1 & q2
    assert not q3(Point(measurement="cities"))
    assert not q3(Point(measurement="weather stations"))
    assert not q3(Point(measurement="states"))
    assert not q3(Point(measurement="taco truck count"))
    assert not q3(Point())
    assert hash(q3)

    # Test time query.
    t1 = datetime.now(timezone.utc) - timedelta(days=365 * 1)
    t2 = datetime.now(timezone.utc) + timedelta(days=365 * 1)
    q1, q2 = TimeQuery() >= t1, TimeQuery() <= t2
    q3 = q1 & q2
    assert q3(Point(time=datetime.now(timezone.utc)))
    assert not q3(Point(time=t1 + timedelta(days=365 * 10)))
    assert not q3(Point(time=t1 - timedelta(days=365 * 10)))
    assert hash(q3)


def test_not():
    """Test logical NOT."""
    # Tags
    q1 = TagQuery().city == "la"
    q2 = ~q1

    p1 = Point(tags={"city": "la"})
    p2 = Point(tags={"city": "sf"})

    assert q1(p1)
    assert q2(p2)
    assert not q1(p2)
    assert not q2(p1)

    assert hash(q2)

    # Fields
    q1 = FieldQuery().temperature == 70.0
    q2 = ~q1
    assert q2(Point(fields={"temperature": 71.0}))
    assert not q2(Point(fields={"temperature": 70.0}))
    assert hash(q2)

    # Measurement
    q1 = MeasurementQuery() == "some measurement"
    q2 = ~q1
    assert q2(Point(measurement="some other measurement"))
    assert not q2(Point(measurement="some measurement"))
    assert hash(q2)

    # Time
    t1 = datetime.now(timezone.utc)
    q1 = TimeQuery() == t1
    q2 = ~q1
    assert q2(Point(time=t1 - timedelta(days=1)))
    assert not q2(Point(time=t1))
    assert hash(q2)


def test_regex():
    """Test regex queries."""
    q = TagQuery().val.matches(r"\d{2}\.")

    assert q(Point(tags={"val": "42."}))
    assert not q(Point(tags={"val": "44"}))
    assert not q(Point(tags={"val": "ab."}))
    assert hash(q)

    assert TagQuery().val.matches("ab.")(Point(tags={"val": "ab."}))

    q = MeasurementQuery().search(r"\d+")
    assert q(Point(measurement="ab3"))
    assert not q(Point(measurement="abc"))
    assert not q(Point())
    assert hash(q)

    with pytest.raises(
        RuntimeError, match="Regex query not supported for FieldQuery."
    ):
        FieldQuery().val.matches(r"")(Point())

    with pytest.raises(
        RuntimeError, match="Regex query not supported for FieldQuery."
    ):
        TimeQuery().matches(r"")(Point())

    with pytest.raises(
        RuntimeError, match="Regex query not supported for FieldQuery."
    ):
        FieldQuery().val.search(r"")(Point())

    with pytest.raises(
        RuntimeError, match="Regex query not supported for FieldQuery."
    ):
        TimeQuery().search(r"")(Point())


def test_custom_function():
    """Test custom function in the test method."""
    # Test an arbitrary function with no additional args.
    def is_los_angeles(value):
        """Return value is los angeles."""
        return value == "los angeles"

    q = TagQuery().city.test(is_los_angeles)
    assert q(Point(tags={"city": "los angeles"}))
    assert not q(Point(tags={"city": "san francisco"}))
    assert not q(Point(tags={"state": "alaska"}))
    assert hash(q)

    # Test an arbitrary function thats takes additional args.
    def is_city(city, matching_city):
        """Return value is some arbitrary city."""
        return city == matching_city

    q = TagQuery().city.test(is_city, "los angeles")
    assert q(Point(tags={"city": "los angeles"}))
    assert not q(Point(tags={"city": "san francisco"}))
    assert not q(Point(tags={"state": "alaska"}))
    assert hash(q)

    # Test an arbitrary function on field.
    def is_freezing(value):
        """Return value is less than or equal to value."""
        return value <= 32.0

    q = FieldQuery().temperature.test(is_freezing)
    assert q(Point(fields={"temperature": 32.0}))
    assert not q(Point(fields={"temperature": 72.0}))
    assert not q(Point())
    assert hash(q)


def test_noop():
    """Test noop method."""
    query = TagQuery().noop()

    assert query(Point())


def test_hash():
    """Test hashing of a query instance."""
    q1 = TagQuery().a == "b"
    q2 = FieldQuery().t >= 0.0
    q3 = MeasurementQuery() == "m"
    q4 = TimeQuery() != datetime.now(timezone.utc)

    s = {q1, q2, q3, q4}

    for q in (q1, q2, q3, q4):
        assert q in s

    for c1, c2 in combinations((q1, q2, q3, q4), 2):
        p1 = c1 & c2
        p2 = c1 | c2
        s.add(p1)
        s.add(p2)

    # Commutative property of & and |
    for c1, c2 in combinations((q1, q2, q3, q4), 2):
        p1 = c2 & c1
        p2 = c2 | c1
        assert p1 in s
        assert p2 in s

    t = datetime.now(timezone.utc)
    q1, q2 = TimeQuery() == t, TimeQuery() > t
    q3 = q1 & q2
    q4 = q2 & q1
    q6, q7 = q1 | q2, q2 | q1

    assert hash(BaseQuery()) is not None
    assert q3._hash is not None
    assert q1._hash != q2._hash
    assert q1 != q2
    assert q3._hash != q1._hash
    assert q1 != q3
    assert q2._hash != q3._hash
    assert q2 != q3
    assert q3._hash == q4._hash
    assert q3 == q4
    assert q6 == q7

    # Test compound queries with unhashable simple queries.
    q5 = TagQuery().map(lambda x: x) == "b"
    assert not q5.is_hashable()
    assert not (q1 & q5).is_hashable()
    assert not (q1 | q5).is_hashable()
    assert not (~q5).is_hashable()
    assert not ((q1 & q5) & q5).is_hashable()
    assert not ((q1 & q5) | q5).is_hashable()
    assert not (~(q1 & q5)).is_hashable()


def test_empty_query_evaluation():
    """Test calling an empty query raises Exception."""
    with pytest.raises(TypeError):
        t = TagQuery()
        t(Point())

    with pytest.raises(TypeError):
        (FieldQuery())(Point())


def test_path_required():
    """Test calling a TagQuery or FieldQuery without keys."""
    with pytest.raises(
        RuntimeError,
        match="Query has no path. Provide tag or field key to query.",
    ):
        t = TagQuery() == "my tag"
        t(Point)

    with pytest.raises(
        RuntimeError,
        match="Query has no path. Provide tag or field key to query.",
    ):
        t = FieldQuery() == 10.0
        t(Point)


def test_path_not_required():
    """Test calling a MeasurementQuery or TimeQuery with keys."""
    with pytest.raises(
        RuntimeError, match="This query does not require a key."
    ):
        MeasurementQuery().whoops == "my tag"

    with pytest.raises(
        RuntimeError, match="This query does not require a key."
    ):
        TimeQuery().oopsies == 10.0


def test_getitem():
    """Test __getattr__ against __getitem__."""
    assert (TagQuery().a == "a") == (TagQuery()["a"] == "a")
    assert (FieldQuery().a == 1) == (FieldQuery()["a"] == 1)


def test_none():
    """Test queries against None values."""
    # None as RHS.
    t = datetime.now()

    q1 = TimeQuery() == None  # noqa: E711
    q2 = MeasurementQuery() == None  # noqa: E711
    q3 = TagQuery().a == None  # noqa: E711
    q4 = FieldQuery().a == None  # noqa: E711
    q5 = FieldQuery().a > 1
    q6 = TagQuery().a == "a"  # noqa: E711

    p1 = Point(time=t, tags={"a": None})
    p2 = Point(time=t, fields={"a": None})

    assert not q1(p1)
    assert not q2(p1)
    assert q3(p1)
    assert not q3(p2)
    assert q4(p2)
    assert not q4(p1)
    assert not q5(p2)
    assert not q6(p1)


def test_compoundquery_and():
    """Test the and method of a CompoundQuery."""
    q1 = TimeQuery() == datetime.now()
    q2 = TagQuery().a == "b"
    q3 = MeasurementQuery() == "m"
    q4 = FieldQuery().a == 1

    c1 = q1 & q2

    assert isinstance(c1, CompoundQuery)
    assert isinstance(c1.query1, SimpleQuery)
    assert isinstance(c1.query2, SimpleQuery)
    assert c1.is_hashable()
    assert c1.operator == operator.and_
    assert (q1 & q2) == (q2 & q1)
    assert not c1 == 1

    c2 = c1 & q3

    assert isinstance(c2, CompoundQuery)
    assert isinstance(c2.query1, CompoundQuery)
    assert isinstance(c2.query2, SimpleQuery)
    assert c2.is_hashable()
    assert c2.operator == operator.and_

    with pytest.raises(RuntimeError):
        q1 & TimeQuery()

    with pytest.raises(RuntimeError):
        TimeQuery() & q4

    with pytest.raises(RuntimeError):
        CompoundQuery(TimeQuery(), q4, operator.and_, "")

    with pytest.raises(RuntimeError):
        CompoundQuery(q4, TimeQuery(), operator.and_, "")


def test_compoundquery_or():
    """Test the or method of a CompoundQuery."""
    q1 = TimeQuery() == datetime.now()
    q2 = TagQuery().a == "b"
    q3 = MeasurementQuery() == "m"
    q4 = FieldQuery().a == 1

    c1 = q1 | q2

    assert isinstance(c1, CompoundQuery)
    assert isinstance(c1.query1, SimpleQuery)
    assert isinstance(c1.query2, SimpleQuery)
    assert c1.is_hashable()
    assert c1.operator == operator.or_
    assert (q1 | q2) == (q2 | q1)
    assert not c1 == 1

    c2 = c1 | q3

    assert isinstance(c2, CompoundQuery)
    assert isinstance(c2.query1, CompoundQuery)
    assert isinstance(c2.query2, SimpleQuery)
    assert c2.is_hashable()
    assert c2.operator == operator.or_

    with pytest.raises(RuntimeError):
        q1 | TimeQuery()

    with pytest.raises(RuntimeError):
        TimeQuery() | q4

    with pytest.raises(RuntimeError):
        CompoundQuery(TimeQuery(), q4, operator.or_, "")

    with pytest.raises(RuntimeError):
        CompoundQuery(q4, TimeQuery(), operator.or_, "")


def test_compoundquery_not():
    """Test the invert method of a CompoundQuery."""
    q1 = TimeQuery() == datetime.now()
    c1 = ~q1

    assert isinstance(c1, CompoundQuery)
    assert isinstance(c1.query1, SimpleQuery)
    assert c1.is_hashable()
    assert c1.operator == operator.not_
    assert not c1 == q1

    c2 = ~c1

    assert isinstance(c2, CompoundQuery)
    assert isinstance(c2.query1, CompoundQuery)
    assert c2.is_hashable()
    assert c2.operator == operator.not_

    with pytest.raises(RuntimeError):
        ~TimeQuery()

    with pytest.raises(RuntimeError):
        ~(~TimeQuery())

    with pytest.raises(RuntimeError):
        CompoundQuery(TimeQuery(), None, operator.not_, "")

    with pytest.raises(RuntimeError):
        CompoundQuery(~TimeQuery(), None, operator.not_, "")


def test_basequery():
    """Test BaseQuery."""
    q = BaseQuery()
    assert not q._point_attr
    assert q._path == ()
    assert not q._path_required
    assert not q._hash
    assert not q.is_hashable()

    with pytest.raises(TypeError):
        q()

    assert repr(q) == "BaseQuery()"

    with pytest.raises(
        RuntimeError,
        match=(
            "Query has no defined Point attribute. "
            "You may be attempting to initialize a BaseQuery."
        ),
    ):
        q == 3

    with pytest.raises(
        RuntimeError,
        match=(
            "Query has no defined Point attribute. "
            "You may be attempting to initialize a BaseQuery."
        ),
    ):
        q.test(lambda _: True) == 3

    with pytest.raises(
        RuntimeError,
        match=(
            "Query has no defined Point attribute. "
            "You may be attempting to initialize a BaseQuery."
        ),
    ):
        q.map(lambda _: True) == 3

    with pytest.raises(
        RuntimeError, match="Cannot logical-AND an empty query."
    ):
        q & (TagQuery().a == "b")

    with pytest.raises(
        RuntimeError, match="Cannot logical-OR an empty query."
    ):
        q | (TagQuery().a == "b")

    with pytest.raises(
        RuntimeError, match="Cannot logical-NOT an empty query."
    ):
        ~q


def test_bad_queries():
    """Test that only valid RHS types can form a valid query."""
    # Test a non-datetime right hand side.
    with pytest.raises(TypeError):
        TimeQuery() == 3

    # Test a non-string right hand side.
    with pytest.raises(TypeError):
        MeasurementQuery() == 3

    # Test a non-string right hand side.
    with pytest.raises(TypeError):
        TagQuery().city == ["a", "b"]

    # Test a non-numeric right hand side.
    with pytest.raises(TypeError):
        FieldQuery().a == "a"
