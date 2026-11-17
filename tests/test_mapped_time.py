"""Regression tests for indexed mapped timestamp queries."""

import operator
from datetime import datetime, timedelta, timezone

import pytest

from tinyflux import FieldQuery, Point, TimeQuery, TinyFlux
from tinyflux.index import Index
from tinyflux.storages import CSVStorage, MemoryStorage

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
OPS = [
    operator.eq,
    operator.ne,
    operator.lt,
    operator.le,
    operator.gt,
    operator.ge,
]


def shifted(value):
    """Shift timestamps forward one day."""
    return value + timedelta(days=1)


def reversed_time(value):
    """Reverse timestamp order around the base date."""
    return BASE - (value - BASE)


def partial(value):
    """Leave the second day undefined."""
    if value == BASE + timedelta(days=1):
        raise ValueError("undefined mapping")
    return shifted(value)


@pytest.fixture(params=[MemoryStorage, CSVStorage])
def mapped_db(request, tmp_path):
    """Create each supported storage with unsorted duplicate timestamps."""
    storage = request.param
    args = [] if storage is MemoryStorage else [str(tmp_path / "points.csv")]
    with TinyFlux(*args, storage=storage) as db:
        db.insert_multiple(
            [
                Point(
                    time=BASE + timedelta(days=day),
                    fields={"id": i},
                )
                for i, day in enumerate([2, 0, 1, 0])
            ]
        )
        yield db


def ids(points):
    """Return point identifiers without depending on search order."""
    return sorted(point.fields["id"] for point in points)


@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("mapping", [shifted, reversed_time, partial])
def test_mapped_comparisons(mapped_db, op, mapping):
    """Indexed comparisons agree with direct predicate evaluation."""
    query = op(TimeQuery().map(mapping), BASE + timedelta(days=1))
    points = mapped_db.all()
    assert ids(mapped_db.search(query)) == ids(p for p in points if query(p))


@pytest.mark.parametrize("op", OPS)
def test_unmapped_comparisons_keep_bisect(monkeypatch, op):
    """Raw timestamp comparisons do not scan through predicate evaluation."""
    points = [Point(time=BASE + timedelta(days=day)) for day in [2, 0, 1, 0]]
    index = Index()
    index.build(points)
    query = op(TimeQuery(), BASE + timedelta(days=1))
    expected = {i for i, point in enumerate(points) if query(point)}

    def unexpected_scan(_):
        raise AssertionError("unmapped comparison used predicate scan")

    monkeypatch.setattr(query, "_path_resolver", unexpected_scan)
    monkeypatch.setattr(query, "_test", unexpected_scan)
    assert index._search_timestamps(query) == expected


def test_mapped_composition_remove_update(mapped_db):
    """Composed mapped predicates select the same points for mutations."""
    mapped = TimeQuery().map(shifted) == BASE + timedelta(days=1)
    selected = mapped & (FieldQuery().id == 1)
    assert ids(mapped_db.search(selected)) == [1]
    assert ids(mapped_db.search(mapped | (FieldQuery().id == 0))) == [0, 1, 3]
    assert ids(mapped_db.search(~mapped)) == [0, 2]
    assert mapped_db.update(selected, fields={"changed": 1}) == 1
    assert ids(mapped_db.search(FieldQuery().changed == 1)) == [1]
    assert mapped_db.remove(mapped) == 2
    assert ids(mapped_db.all()) == [0, 2]


def test_partial_mapping_custom_predicate(mapped_db):
    """Mapping exceptions also remain non-matches for custom tests."""
    query = TimeQuery().map(partial).test(lambda value: value == shifted(BASE))
    assert ids(mapped_db.search(query)) == [1, 3]


@pytest.mark.parametrize("operation", ["remove", "update"])
def test_mapped_mutations(mapped_db, operation):
    """Each mutation independently uses mapped rather than raw timestamps."""
    query = TimeQuery().map(shifted) == BASE + timedelta(days=1)
    if operation == "remove":
        assert mapped_db.remove(query) == 2
        assert ids(mapped_db.all()) == [0, 2]
    else:
        assert mapped_db.update(query, fields={"changed": 1}) == 2
        assert ids(mapped_db.search(FieldQuery().changed == 1)) == [1, 3]


def test_chained_mapping_and_empty_index(mapped_db):
    """Apply all maps in sequence, including on an empty index."""
    query = TimeQuery().map(partial).map(shifted) == BASE + timedelta(days=2)
    assert ids(mapped_db.search(query)) == [1, 3]
    index = Index()
    index.build([])
    assert index._search_timestamps(query) == set()
