"""Tests for tinyflux.utils module."""
import pytest

from tinyflux.utils import (
    freeze,
    FrozenDict,
    find_eq,
    find_ge,
    find_gt,
    find_le,
    find_lt,
)


def test_freeze():
    """Test the freeze utility."""
    frozen = freeze([0, 1, 2, {"a": [1, 2, 3]}, {1, 2}])

    assert isinstance(frozen, tuple)
    assert isinstance(frozen[3], FrozenDict)
    assert isinstance(frozen[3]["a"], tuple)
    assert isinstance(frozen[4], frozenset)

    with pytest.raises(TypeError):
        frozen[0] = 10

    with pytest.raises(TypeError):
        frozen[3]["a"] = 10

    with pytest.raises(TypeError):
        frozen[3].pop("a")

    with pytest.raises(TypeError):
        frozen[3].update({"a": 9})


def test_frozen_dict_hash():
    """Tesh the hash function on FrozenDict class."""
    my_frozen_set1 = FrozenDict({"city": "la", "state": "ca"})
    my_frozen_set2 = FrozenDict({"state": "ca", "city": "la"})
    my_frozen_set3 = FrozenDict({"temp": 70})

    assert hash(my_frozen_set1) == hash(my_frozen_set2)
    assert hash(my_frozen_set1) != hash(my_frozen_set3)
    assert hash(my_frozen_set2) != hash(my_frozen_set3)


def test_find_eq():
    """Test the find_eq function."""
    present_numbers = range(3)
    absent_numbers = range(3, 6)

    # Normal sorted list.
    my_list = [i for i in present_numbers]

    for i, n in enumerate(present_numbers):
        assert find_eq(my_list, n) == i

    for i, n in enumerate(absent_numbers):
        assert find_eq(my_list, n) is None

    # Empty list.
    my_list = []

    for n in present_numbers:
        assert find_eq(my_list, n) is None

    for i, n in enumerate(absent_numbers):
        assert find_eq(my_list, n) is None


def test_find_lt():
    """Test the find_lt function."""
    present_numbers = range(3, 6)
    absent_numbers1 = range(3)
    absent_numbers2 = range(6, 9)

    # Normal sorted list.
    my_list = [i for i in present_numbers]

    assert find_lt(my_list, 3) is None
    assert find_lt(my_list, 4) == 0
    assert find_lt(my_list, 5) == 1

    for n in absent_numbers1:
        assert find_lt(my_list, n) is None

    for n in absent_numbers2:
        assert find_lt(my_list, n) == 2

    # Empty list.
    my_list = []

    for n in present_numbers:
        assert find_lt(my_list, n) is None

    for n in absent_numbers1:
        assert find_lt(my_list, n) is None

    for n in absent_numbers2:
        assert find_lt(my_list, n) is None


def test_find_le():
    """Test the find_le function."""
    present_numbers = range(3, 6)
    absent_numbers1 = range(3)
    absent_numbers2 = range(6, 9)

    # Normal sorted list.
    my_list = [i for i in present_numbers]

    for i, n in enumerate(present_numbers):
        assert find_le(my_list, n) == i

    for n in absent_numbers1:
        assert find_le(my_list, n) is None

    for n in absent_numbers2:
        assert find_le(my_list, n) == 2

    # Empty list.
    my_list = []

    for n in present_numbers:
        assert find_le(my_list, n) is None

    for n in absent_numbers1:
        assert find_le(my_list, n) is None

    for n in absent_numbers2:
        assert find_le(my_list, n) is None


def test_find_gt():
    """Test the find_gt function."""
    present_numbers = range(3, 6)
    absent_numbers1 = range(3)
    absent_numbers2 = range(6, 9)

    # Normal sorted list.
    my_list = [i for i in present_numbers]

    assert find_gt(my_list, 3) == 1
    assert find_gt(my_list, 4) == 2
    assert find_gt(my_list, 5) is None

    for n in absent_numbers1:
        assert find_gt(my_list, n) == 0

    for n in absent_numbers2:
        assert find_gt(my_list, n) is None

    # Empty list.
    my_list = []

    for n in present_numbers:
        assert find_gt(my_list, n) is None

    for n in absent_numbers1:
        assert find_gt(my_list, n) is None

    for n in absent_numbers2:
        assert find_gt(my_list, n) is None


def test_find_ge():
    """Test the find_ge function."""
    present_numbers = range(3, 6)
    absent_numbers1 = range(3)
    absent_numbers2 = range(6, 9)

    # Normal sorted list.
    my_list = [i for i in present_numbers]

    for i, n in enumerate(present_numbers):
        assert find_ge(my_list, n) == i

    for n in absent_numbers1:
        assert find_ge(my_list, n) == 0

    for n in absent_numbers2:
        assert find_ge(my_list, n) is None

    # Empty list.
    my_list = []

    for n in present_numbers:
        assert find_ge(my_list, n) is None

    for n in absent_numbers1:
        assert find_ge(my_list, n) is None

    for n in absent_numbers2:
        assert find_ge(my_list, n) is None
