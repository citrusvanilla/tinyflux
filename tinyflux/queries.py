"""Defintion of TinyFlux Queries.

A query contains logic in the form of a test and it acts upon a single Point
when it is eventually evaluated.

All Queries begin as subclass of BaseQuery, which is not itself callable. Logic
for the query is handled by the Python data model of the BaseQuery class,
resulting in the generation of a SimpleQuery, which is callable. SimpleQuery
instances support logical AND, OR, and NOT operations, which result in the
initialization of a new CompoundQuery object.

Each SimpleQuery instance contains attributes that constitute the
"deconstuction" of a query into several key parts (e.g. the operator, the
right-hand side) so that the other consumers of queries, includng an Index, may
use them for their own purposes.
"""
from datetime import datetime
import operator
import re
from typing import (
    Any,
    Callable,
    Mapping,
    Optional,
    Tuple,
    Union,
)
from typing_extensions import TypeAlias

from .point import Point


Query: TypeAlias = Union["SimpleQuery", "CompoundQuery"]


class CompoundQuery:
    """A container class for simple and/or compound queries and an operator.

    A CompoundQuery is generated by built-in __and__, __or__, and __not__
    operations on a SimpleQuery.

    Attributes:
        query1: A SimpleQuery or CompoundQuery instance.
        query2: A SimpleQuery or CompoundQuery instance.
        operator: The operator.

    Usage:
        >>> from tinyflux import FieldQuery, TagQuery
        >>> time_q = FieldQuery().temp_f < 55.0
        >>> tags_q = TagQuery().city == "Los Angeles"
        >>> cold_LA_q = time_q & tags_q
        >>> type(cold_LA_q)
        <class 'tinyflux.queries.CompoundQuery'>
    """

    def __init__(
        self,
        query1: Query,
        query2: Optional[Query],
        operator: Callable[..., Any],
        hashval: Optional[Tuple],
    ) -> None:
        """Initialize an CompoundQuery.

        Args:
            query1: A SimpleQuery or CompoundQuery instance.
            query2: A SimpleQuery or CompoundQuery instance.
            operator: A callable from the operator module.
            hashval: The hash value for the query.
        """
        if not isinstance(query1, (CompoundQuery, SimpleQuery)):
            raise RuntimeError(
                "One or more queries is not a CompoundQuery or SimpleQuery."
            )
        if query2 and not isinstance(query2, (CompoundQuery, SimpleQuery)):
            raise RuntimeError(
                "One or more queries is not a CompoundQuery or SimpleQuery."
            )

        self.query1 = query1
        self.query2 = query2
        self.operator = operator
        self._hash = hashval

    def __call__(self, point: Point) -> bool:
        """Evaluate the query against a Point.

        Args:
            point: A Point object.

        Returns:
            A boolean for the result of the query evaluation.
        """
        if self.query2:
            return self.operator(self.query1(point), self.query2(point))

        return self.operator(self.query1(point))

    def __hash__(self) -> int:
        """Get a hash value for this query."""
        return hash(self._hash)

    def __repr__(self) -> str:
        """Return printable representation of CompoundQuery."""
        if self.query1 and self.query2:
            return (
                f"CompoundQuery({self.operator.__name__}, "
                f"{repr(self.query1.__class__.__name__)}, "
                f"{repr(self.query2.__class__.__name__)})"
            )
        else:
            return (
                f"CompoundQuery({self.operator.__name__}, "
                f"{repr(self.query1.__class__.__name__)})"
            )

    def __eq__(self, other: object) -> bool:
        """Test equality of this CompoundQuery and another object."""
        if (
            isinstance(other, (CompoundQuery, SimpleQuery))
            and self._hash
            and other._hash
        ):
            return self._hash == other._hash
        else:
            return False

    def __and__(self, other: Query) -> "CompoundQuery":
        """Combine this query with another using logical-AND.

        Args:
            other: A SimpleQuery or CompoundQuery instance.

        Returns:
            A CompoundQuery instance.
        """
        if self.is_hashable() and other.is_hashable():
            hashval = ("and_", frozenset([self._hash, other._hash]))
        else:
            hashval = None

        return CompoundQuery(self, other, operator.and_, hashval)

    def __or__(self, other: Query) -> "CompoundQuery":
        """Combine this query with another using logical-OR.

        Args:
            other: A SimpleQuery or CompoundQuery instance.

        Returns:
            A CompoundQuery instance.
        """
        if self.is_hashable() and other.is_hashable():
            hashval = ("or_", frozenset([self._hash, other._hash]))
        else:
            hashval = None

        return CompoundQuery(self, other, operator.or_, hashval)

    def __invert__(self) -> "CompoundQuery":
        """Combine this query with another using logical-NOT.

        Returns:
            A CompoundQuery instance.
        """
        if self.is_hashable():
            hashval = ("not_", self._hash)
        else:
            hashval = None

        return CompoundQuery(self, None, operator.not_, hashval)

    def is_hashable(self) -> bool:
        """Return the ability to hash this query."""
        return self._hash is not None


class SimpleQuery:
    """A single query instance.

    This is the object on which the actual query operations are performed. The
    BaseQuery class acts like a query builder and generates SimpleQuery objects
    which will evaluate their query against a given point when called.

    Query instances can be combined using logical OR and AND and inverted using
    logical NOT.

    A SimpleQuery can be parsed using private attributes.

    TODO:
    In order to be usable in a query cache, a query needs to have a stable hash
    value with the same query always returning the same hash. That way a query
    instance can be used as a key in a dictionary.

    Usage:
        >>> from tinyflux import TagQuery
        >>> los_angeles_q = TagQuery().city == "Los Angeles"
        >>> type(los_angeles_q)
        <class 'tinyflux.queries.SimpleQuery'>
    """

    def __init__(
        self,
        point_attr: str,
        operator: Callable[..., Any],
        rhs: Any,
        test: Callable[..., bool],
        path_resolver: Callable[..., Union[int, float, str, None]],
        hashval: Optional[Tuple],
    ) -> None:
        """Initialize an SimpleQuery.

        Args:
            point_attr: The attribute of a Point relevant for this query.
            operator: The operator portion of a test.
            rhs: The value that a test should evaluated against.
            test: The combined logic as a callable.
            path_resolver: A fetcher/translater for Point metadata that the
                           query will be evaluated on.
            hashval: The hash value for the query.
        """
        self._point_attr = point_attr
        self._operator = operator
        self._rhs = rhs
        self._test = test
        self._path_resolver = path_resolver
        self._hash = hashval

    @property
    def point_attr(self) -> str:
        """Get the attribute of a Point object relevant for this query."""
        return self._point_attr

    def __call__(self, point: Optional[Point] = None) -> bool:
        """Evaluate the query against the point.

        Args:
            point: The point to test

        Returns:
            Whether the point matches this query.
        """
        obj_attr = getattr(point, self._point_attr)

        try:
            value = self._path_resolver(obj_attr)
        except Exception:
            return False

        return self._test(value)

    def __hash__(self) -> int:
        """Hash this query.

        Returns:
            The hash value.
        """
        return hash(self._hash)

    def __repr__(self) -> str:
        """Return printable representation of SimpleQuery."""
        if self._hash:
            return f"SimpleQuery{self._hash}"
        else:
            return "SimpleQuery()"

    def __eq__(self, other: object) -> bool:
        """Test equality of this SimpleQuery and another object."""
        if isinstance(other, SimpleQuery) and self._hash and other._hash:
            return self._hash == other._hash
        else:
            return False

    def __and__(self, other: Query) -> "CompoundQuery":
        """Combine this query with another using logical-AND.

        Args:
            other: A SimpleQuery or CompoundQuery instance.

        Returns:
            A CompoundQuery instance.
        """
        if self.is_hashable() and other.is_hashable():
            hashval = ("and", frozenset([self._hash, other._hash]))
        else:
            hashval = None

        return CompoundQuery(self, other, operator.and_, hashval)

    def __or__(self, other: Query) -> "CompoundQuery":
        """Combine this query with another using logical-OR.

        Args:
            other: A SimpleQuery or CompoundQuery instance.

        Returns:
            A CompoundQuery instance.
        """
        if self.is_hashable() and other.is_hashable():
            hashval = ("or", frozenset([self._hash, other._hash]))
        else:
            hashval = None

        return CompoundQuery(self, other, operator.or_, hashval)

    def __invert__(self) -> "CompoundQuery":
        """Combine this query with another using logical-NOT.

        Returns:
            A CompoundQuery instance.
        """
        if self.is_hashable():
            hashval = ("not", self._hash)
        else:
            hashval = None

        return CompoundQuery(self, None, operator.not_, hashval)

    def is_hashable(self) -> bool:
        """Return the ability to hash this query."""
        return self._hash is not None


class BaseQuery:
    """A base class for the different TinyFlux query types.

    A query type that explicity unifies the divergent interfaces of TimeQuery,
    MeasurementQuery, TagQuery, and FieldQuery.

    A BaseQuery is not iteslf callable. When it is combined with test logic,
    it generates a SimpleQuery, which is callable without exception.

    Usage:
        >>> from tinyflux import TagQuery, Point
        >>> p = Point(tags={"city": "LA"})
        >>> q1 = TagQuery()
        >>> isinstance(q1, tinyflux.queries.BaseQuery)
        True
        >>> q1(p)
        RuntimeError: Empty query was evaluated.
        >>> q2 = TagQuery().city == "LA"
        >>> q2
        SimpleQuery('tags', '==', ('city',), 'LA')
        >>> q2(p)
        True
    """

    def __init__(self) -> None:
        """Initialize a BaseQuery.

        There is no need to initialize a BaseQuery directly.

        Args:
            point_attr: The attribute of a Point relevant for this query.
            path_required: This query requires a key.
        """
        self._point_attr: Optional[str] = None
        self._path: Tuple[Union[str, Callable], ...] = ()
        self._path_required = False
        self._hash: Optional[Tuple] = None

    def __repr__(self):
        """Return printable representation of BaseQuery."""
        return f"{type(self).__name__}()"

    def __hash__(self) -> int:
        """Hash this query.

        Returns:
            The hash value.
        """
        return hash(self._hash)

    def __getattr__(self, item: str) -> "BaseQuery":
        """Generate a new query object with the new query key.

        Args:
            item: The attribute.

        Returns:
            A new BaseQuery instance.
        """
        # We use type(self) to get the class of the current query in case
        # someone uses a subclass of BaseQuery.
        if not self._path_required:
            raise RuntimeError("This query does not require a key.")

        # Build new BaseQuery.
        query = type(self)()
        query._point_attr = self._point_attr
        query._path_required = self._path_required
        query._path = self._path + (item,)
        query._hash = ("path", query._path) if self.is_hashable() else None

        return query

    def __getitem__(self, item: str) -> "BaseQuery":
        """Get attribute with the __getitem__ syntax.

        A different syntax for ``__getattr__``

        Args:
            item: The attribute.

        Returns:
            A new BaseQuery instance.
        """
        return self.__getattr__(item)

    def _generate_simple_query(
        self,
        operator: Callable[..., Any],
        test_against_rhs: bool,
        rhs: Any,
        args: Any,
        hashval: Tuple,
    ) -> SimpleQuery:
        """Generate a SimpleQuery and its components.

        A helper function for a BaseQuery instance.

        Args:
            operator: A Callable.
            rhs: A value to test against.
            test_against_rhs: Whether the test should evaulate against RHS.
            hashval: The hash value for the query.
        """
        # Make sure this query has some keys if they are required.
        if self._path_required and not self._path:
            raise RuntimeError(
                "Query has no path. Provide tag or field key to query."
            )

        # Make sure this query operates on a Point attribute.
        if not self._point_attr:
            raise RuntimeError(
                "Query has no defined Point attribute. "
                "You may be attempting to initialize a BaseQuery."
            )

        # Validation for time.
        if (
            self._point_attr == "_time"
            and rhs
            and not isinstance(rhs, datetime)
        ):
            raise TypeError(
                "TimeQuery comparison value must be datetime object."
            )

        # Validation for measurement.
        if (
            self._point_attr == "_measurement"
            and rhs
            and not isinstance(rhs, str)
        ):
            raise TypeError(
                "MeasurementQuery comparison value must be string."
            )

        # Validation for tags.
        if self._point_attr == "_tags" and rhs and not isinstance(rhs, str):
            raise TypeError("TagQuery comparison value must be string.")

        # Validation for fields.
        if (
            self._point_attr == "_fields"
            and rhs
            and not isinstance(rhs, (int, float))
        ):
            raise TypeError("FieldQuery comparison value must be numeric.")

        def test(x: Any) -> bool:
            """The test function from an operator and righthand side."""
            if not test_against_rhs:
                return operator(x, *args) if args else operator(x)

            # Wrap this in a try/except block.
            # Some operators do not work against None types.
            # They should evaluate to False.
            try:
                return operator(x, rhs)
            except Exception:
                return False

        def path_resolver(value: Any) -> Any:
            """Get the correct value from an object to execute a test upon.

            Raises:
                Exception if the path cannot be resolved.
            """
            try:
                # Resolve path for mappings.
                for part in self._path:

                    # Normal key/val traversal.
                    if isinstance(part, str):
                        value = value[part]

                    # Function in the path.
                    else:
                        value = part(value)

                return value

            except Exception as e:
                raise e

        return SimpleQuery(
            point_attr=self._point_attr,
            operator=operator,
            rhs=rhs,
            test=test,
            path_resolver=path_resolver,
            hashval=hashval if self.is_hashable() else None,
        )

    def __eq__(self, rhs: Any) -> SimpleQuery:  # type: ignore
        """Override the equality method.

        This violates LSP.  We know.

        Args:
            rhs: The value to compare against.

        Usage:
            >>> TagQuery().my_tag == "my tag value"
        """
        return self._generate_simple_query(
            operator=operator.eq,
            test_against_rhs=True,
            rhs=rhs,
            args=None,
            hashval=(self._point_attr, "==", self._path, rhs),
        )

    def __ne__(self, rhs: Any) -> SimpleQuery:  # type: ignore
        """Override the not-equals method.

        This violates LSP.  We know.

        Args:
            rhs: The value to compare against.

        Usage:
            >>> TagQuery().my_tag != "your tag value"
        """
        return self._generate_simple_query(
            operator=operator.ne,
            test_against_rhs=True,
            rhs=rhs,
            args=None,
            hashval=(self._point_attr, "!=", self._path, rhs),
        )

    def __lt__(self, rhs: Any) -> SimpleQuery:
        """Override the less-than method.

        Args:
            rhs: The value to compare against.

        Usage:
            >>> FieldQuery().my_field < 100
        """
        return self._generate_simple_query(
            operator=operator.lt,
            test_against_rhs=True,
            rhs=rhs,
            args=None,
            hashval=(self._point_attr, "<", self._path, rhs),
        )

    def __le__(self, rhs: Any) -> SimpleQuery:
        """Override the less-than-or-equals method.

        Args:
            rhs: The value to compare against.

        Usage:
            >>> FieldQuery().my_field <= 100
        """
        return self._generate_simple_query(
            operator=operator.le,
            test_against_rhs=True,
            rhs=rhs,
            args=None,
            hashval=(self._point_attr, "<=", self._path, rhs),
        )

    def __gt__(self, rhs: Any) -> SimpleQuery:
        """Override the greater-than method.

        Args:
            rhs: The value to compare against.

        Usage:
            >>> FieldQuery().my_field > 100
        """
        return self._generate_simple_query(
            operator=operator.gt,
            test_against_rhs=True,
            rhs=rhs,
            args=None,
            hashval=(self._point_attr, ">", self._path, rhs),
        )

    def __ge__(self, rhs: Any) -> SimpleQuery:
        """Override the greater-than-or-equals method.

        Args:
            rhs: The value to compare against.

        Usage:
            >>> FieldQuery().my_field >= 100
        """
        return self._generate_simple_query(
            operator=operator.ge,
            test_against_rhs=True,
            rhs=rhs,
            args=None,
            hashval=(self._point_attr, ">=", self._path, rhs),
        )

    def __and__(self, other: Any) -> None:
        """Override the and method.

        Raises a RuntimeError with user-friendly directive.

        Args:
            other: The value to compare against.
        """
        raise RuntimeError("Cannot logical-AND an empty query.")

    def __or__(self, other: Any) -> None:
        """Override the or method.

        Raises a RuntimeError with user-friendly directive.

        Args:
            other: The value to compare against.
        """
        raise RuntimeError("Cannot logical-OR an empty query.")

    def __invert__(self) -> None:
        """Override the invert method.

        Raises a RuntimeError with user-friendly directive.
        """
        raise RuntimeError("Cannot logical-NOT an empty query.")

    def test(self, func: Callable[[Mapping], bool], *args) -> SimpleQuery:
        """Run a user-defined test function against a value.

        >>> def test_func(val):
        ...     return val == 42
        ...
        >>> FieldQuery()["my field"].test(test_func)

        Warning:
            The test fuction provided needs to be deterministic (returning the
            same value when provided with the same arguments), otherwise this
            may mess up the query cache that :class:`~tinyflux.table.Table`
            implements.

        Args:
            func: The function to call, passing the value as the first arg.
            args: Additional arguments to pass to the test function.
        """
        return self._generate_simple_query(
            operator=func,
            test_against_rhs=False,
            rhs=None,
            args=args,
            hashval=(self._point_attr, "test", self._path, func, args),
        )

    def is_hashable(self) -> bool:
        """Return hash is not empty."""
        return self._hash is not None

    def matches(self, regex: str, flags: int = 0) -> SimpleQuery:
        r"""Run a regex test against a value (whole string has to match).

        >>> TagQuery().f1.matches(r'^\\w+$')

        Args:
            regex: The regular expression to use for matching.
            flags: Regex flags to pass to re.match.
        """

        def test(value: Any) -> bool:
            """Test function."""
            return re.match(regex, value, flags) is not None

        return self._generate_simple_query(
            operator=test,
            test_against_rhs=False,
            rhs=None,
            args=None,
            hashval=(self._point_attr, "matches", self._path, regex),
        )

    def search(self, regex: str, flags: int = 0) -> SimpleQuery:
        r"""Run a regex test against a value (only substring string has to match).

        >>> TagQuery().f1.search(r'^\\w+$')

        Args:
            regex: The regular expression to use for matching.
            flags: Regex flags to pass to re.match.
        """

        def test(value: Any) -> bool:
            """Test function."""
            return re.search(regex, value, flags) is not None

        return self._generate_simple_query(
            operator=test,
            test_against_rhs=False,
            rhs=None,
            args=None,
            hashval=(self._point_attr, "search", self._path, regex),
        )

    def noop(self) -> SimpleQuery:
        """Evaluate to True.

        Useful for having a base value when composing queries dynamically.
        """
        return SimpleQuery(
            point_attr=self._point_attr or "",
            operator=lambda _: True,
            rhs=None,
            test=lambda _: True,
            path_resolver=lambda x: x,
            hashval=(),
        )

    def map(self, func: Callable[[Any], Any]) -> "BaseQuery":
        """Add a function to the query path.

        Similar to __getattr__ but for arbitrary functions.

        Args:
            func: The function to add.
        """
        # Init a new BaseQuery.
        query = type(self)()
        query._point_attr = self._point_attr
        query._path_required = self._path_required

        # Add the callable to the query path.
        query._path = self._path + (func,)

        # Kill the hash - callable objects can be mutable so it's
        # harmful to cache their results.
        query._hash = None

        return query


class TagQuery(BaseQuery):
    """The base query for Point tags.

    Generates a SimpleQuery that evaluates Point 'tags' attributes.

    Usage:
        >>> from tinyflux import TagQuery
        >>> my_tag_q = TagQuery().my_tag_key == "my tag value"
    """

    def __init__(self) -> None:
        """Initialize a TagQuery instance."""
        super().__init__()
        self._point_attr = "_tags"
        self._path_required = True
        self._hash = ("tags",)

    def exists(self) -> SimpleQuery:
        """Test a Point where a provided key exists.

        >>> TagQuery().my_tag.exists()
        """
        return self._generate_simple_query(
            operator=lambda _: True,
            test_against_rhs=False,
            rhs=None,
            args=None,
            hashval=(self._point_attr, "exists", self._path),
        )


class FieldQuery(BaseQuery):
    """The base query for Point fields.

    Generates a SimpleQuery that evaluates Point 'fields' attributes.

    Usage:
        >>> from tinyflux import FieldQuery
        >>> my_field_q = FieldQuery().my_field == 10.0
    """

    def __init__(self) -> None:
        """Initialize a FieldQuery instance."""
        super().__init__()
        self._point_attr = "_fields"
        self._path_required = True
        self._hash = ("fields",)

    def exists(self) -> SimpleQuery:
        """Test at Point where a provided key exists.

        Usage:
            >>> FieldQuery().my_field.exists()
        """
        return self._generate_simple_query(
            operator=lambda _: True,
            test_against_rhs=False,
            rhs=None,
            args=None,
            hashval=(self._point_attr, "exists", self._path),
        )

    def matches(self, regex: str, flags: int = 0) -> SimpleQuery:
        """Raise an exception for regex query."""
        raise RuntimeError("Regex query not supported for FieldQuery.")

    def search(self, regex: str, flags: int = 0) -> SimpleQuery:
        """Raise an exception for regex query."""
        raise RuntimeError("Regex query not supported for FieldQuery.")


class MeasurementQuery(BaseQuery):
    """The base query for Point measurement.

    Generates a SimpleQuery that evaluates Point 'measurement' attributes.

    Usage:
        >>> from tinyflux import MeasurementQuery
        >>> my_measurement_q = MeasurementQuery() == "my measurement"
    """

    def __init__(self) -> None:
        """Initialize a MeasurementQuery instance."""
        super().__init__()
        self._point_attr = "_measurement"
        self._path_required = False
        self._hash = ("measurement",)


class TimeQuery(BaseQuery):
    """The base query for Point time.

    Generates a SimpleQuery that evaluates Point 'measurement' attributes.

    Usage:
        >>> from datetime import datetime, timezone
        >>> from tinyflux import TimeQuery
        >>> my_time_q = TimeQuery() < datetime.now(timezone.utc)
    """

    def __init__(self) -> None:
        """Initialize a TimeQuery instance."""
        super().__init__()
        self._point_attr = "_time"
        self._path_required = False
        self._hash = ("time",)

    def matches(self, regex: str, flags: int = 0) -> SimpleQuery:
        """Raise an exception for regex query."""
        raise RuntimeError("Regex query not supported for FieldQuery.")

    def search(self, regex: str, flags: int = 0) -> SimpleQuery:
        """Raise an exception for regex query."""
        raise RuntimeError("Regex query not supported for FieldQuery.")
