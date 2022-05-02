"""Defintion of TinyFlux utils."""
import bisect
from typing import Generator, List, Optional


class FrozenDict(dict):
    """
    An immutable dictionary.

    This is used to generate stable hashes for queries that contain dicts.
    Usually, Python dicts are not hashable because they are mutable. This
    class removes the mutability and implements the ``__hash__`` method.
    """

    def __hash__(self):
        """Hash the value of a FrozenDict instance."""
        # Calculate the has by hashing a tuple of all dict items
        return hash(tuple(sorted(self.items())))

    def _immutable(self, *args, **kws):
        """Raise a TypeError for a given dict method."""
        raise TypeError("object is immutable")

    # Disable write access to the dict
    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    setdefault = _immutable
    popitem = _immutable

    def update(self, e=None, **f):
        """Raise TypeError for update."""
        raise TypeError("object is immutable")

    def pop(self, k, d=None):
        """Raise TypeError for pop."""
        raise TypeError("object is immutable")


def freeze(obj: object) -> object:
    """Freeze an object by making it immutable and thus hashable.

    Args:
        obj: Any python object.

    Returns:
        The object in a hashable form.
    """
    if isinstance(obj, dict):
        return FrozenDict((k, freeze(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return tuple(freeze(i) for i in obj)
    elif isinstance(obj, set):
        return frozenset(obj)
    else:
        return obj


def find_eq(sorted_list: List, x: int) -> Optional[int]:
    """Locate the leftmost value exactly equal to x.

    Args:
        sorted_list: The list to search.
        x: The element to search.

    Returns:
        The index of the found element or None.
    """
    i = bisect.bisect_left(sorted_list, x)

    if i != len(sorted_list) and sorted_list[i] == x:
        return i

    return None


def find_lt(sorted_list: List, x: int) -> Optional[int]:
    """Find rightmost value less than x.

    Args:
        sorted_list: The list to search.
        x: The element to search.

    Returns:
        The index of the found element or None.
    """
    i = bisect.bisect_left(sorted_list, x)

    if i:
        return i - 1

    return None


def find_le(sorted_list: List, x: int) -> Optional[int]:
    """Find rightmost value less than or equal to x.

    Args:
        sorted_list: The list to search.
        x: The element to search.

    Returns:
        The index of the found element or None.
    """
    i = bisect.bisect_right(sorted_list, x)

    if i:
        return i - 1

    return None


def find_gt(sorted_list: List, x: int) -> Optional[int]:
    """Find leftmost value greater than x.

    Args:
        sorted_list: The list to search.
        x: The element to search.

    Returns:
        The index of the found element or None.
    """
    i = bisect.bisect_right(sorted_list, x)

    if i != len(sorted_list):
        return i

    return None


def find_ge(sorted_list: List, x: int) -> Optional[int]:
    """Find leftmost item greater than or equal to x.

    Args:
        sorted_list: The list to search.
        x: The element to search.

    Returns:
        The index of the found element or None.
    """
    i = bisect.bisect_left(sorted_list, x)

    if i != len(sorted_list):
        return i

    return None


def difference_generator_and_sorted_lists(
    generator: Generator, inp_list: List
):
    """ """
    rst = []
    j = 0

    for i in generator:
        # Run out rest of generator.
        if j == len(inp_list):
            rst.append(i)
            continue

        # Match. Skip
        if i == inp_list[j]:
            j += 1

        # Not a match. Keep.
        else:
            rst.append(i)

    return rst


def intersection_two_sorted_lists(list1, list2):
    """ """
    rst = []
    i, j = 0, 0

    while i < len(list1) and j < len(list2):
        # Duplicates, keep.
        if list1[i] == list2[j]:
            rst.append(list1[i])
            i += 1
            j += 1
            continue

        if list1[i] > list2[j]:
            j += 1
        else:
            i += 1

    return rst


def union_two_sorted_lists(list1, list2):
    """ """
    rst = []
    i, j = 0, 0

    while i < len(list1) and j < len(list2):
        # Duplicates. Skip.
        if list1[i] == list2[j]:
            i += 1
            continue

        if list1[i] > list2[j]:
            rst.append(list2[j])
            j += 1
        else:
            rst.append(list1[i])
            i += 1

    while i < len(list1):
        rst.append(list1[i])
        i += 1

    while j < len(list2):
        rst.append(list2[j])
        j += 1

    return rst
