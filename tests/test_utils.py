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
    difference_generator_and_sorted_lists,
    intersection_two_sorted_lists,
    union_two_sorted_lists,
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


def test_intersection_two_sorted_lists():
    """Test the intersection of two sorted lists."""
    # Test same lists.
    l1, l2 = [1, 2, 3, 4, 5], [1, 2, 3, 4, 5]
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5]

    # L1 empty.
    l1, l2 = [], [1, 2, 3, 4, 5]
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == []

    # L2 empty.
    l1, l2 = [1, 2, 3, 4, 5], []
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == []

    # Both empty.
    l1, l2 = [], []
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == []

    # Test L1 all less than L2.
    l1, l2 = [1, 2, 3, 4, 5], [6, 7, 8, 9, 10]
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == []

    # Test L2 all less than L1.
    l1, l2 = [6, 7, 8, 9, 10], [1, 2, 3, 4, 5]
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == []

    # Test needle in a haystack.
    l1, l2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [1]
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == [1]

    # Test needle in a haystack.
    l1, l2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [11]
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == []

    # Test needle in a haystack.
    l1, l2 = [1, 2, 3, 4, 5, 7, 8, 9, 10], [6]
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == []

    # Test overlaps.
    l1, l2 = [1, 2, 3, 4, 5], [3, 4, 5, 6]
    rst = intersection_two_sorted_lists(l1, l2)
    assert rst == [3, 4, 5]


def test_union_two_sorted_lists():
    """Test the union of two sorted lists."""
    # Test same lists.
    l1, l2 = [1, 2, 3, 4, 5], [1, 2, 3, 4, 5]
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5]

    # L1 empty.
    l1, l2 = [], [1, 2, 3, 4, 5]
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5]

    # L2 empty.
    l1, l2 = [1, 2, 3, 4, 5], []
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5]

    # Both empty.
    l1, l2 = [], []
    rst = union_two_sorted_lists(l1, l2)
    assert rst == []

    # Test L1 all less than L2.
    l1, l2 = [1, 2, 3, 4, 5], [6, 7, 8, 9, 10]
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Test L2 all less than L1.
    l1, l2 = [6, 7, 8, 9, 10], [1, 2, 3, 4, 5]
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Test needle in a haystack.
    l1, l2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [1]
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Test needle in a haystack.
    l1, l2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [11]
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

    # Test needle in a haystack.
    l1, l2 = [1, 2, 3, 4, 5, 7, 8, 9, 10], [6]
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Test overlaps.
    l1, l2 = [1, 2, 3, 4, 5], [3, 4, 5, 6]
    rst = union_two_sorted_lists(l1, l2)
    assert rst == [1, 2, 3, 4, 5, 6]


def test_difference_generator_and_sorted_lists():
    """Test the difference of two sorted lists."""
    # Test empty l.
    g = range(5)
    my_list = []
    rst = difference_generator_and_sorted_lists(g, my_list)
    assert rst == [0, 1, 2, 3, 4]

    # Test empty gen.
    g = range(0)
    my_list = [1, 2]
    rst = difference_generator_and_sorted_lists(g, my_list)
    assert rst == []

    # Test equal inputs.
    g = range(2)
    my_list = [0, 1]
    rst = difference_generator_and_sorted_lists(g, my_list)
    assert rst == []

    # Test normal cases.
    g = range(5)
    my_list = [0, 1]
    rst = difference_generator_and_sorted_lists(g, my_list)
    assert rst == [2, 3, 4]

    g = range(5)
    my_list = [2, 3, 4]
    rst = difference_generator_and_sorted_lists(g, my_list)
    assert rst == [0, 1]

    g = range(5)
    my_list = [2, 3]
    rst = difference_generator_and_sorted_lists(g, my_list)
    assert rst == [0, 1, 4]
