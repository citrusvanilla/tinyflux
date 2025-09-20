"""Tests for the tinyflux.point module."""

from datetime import datetime, timezone, timedelta
import pytest
from tinyflux.point import Point, validate_tags, validate_fields


@pytest.fixture
def test_times():
    """Create a collection of test datetime objects."""
    now = datetime.now(timezone.utc)
    return {
        "now": now,
        "past": now - timedelta(days=1),
        "future": now + timedelta(days=1),
        "year_2000": datetime.strptime(
            "01-01-2000 00:00:00", "%m-%d-%Y %H:%M:%S"
        ),
        "year_3000": datetime.strptime(
            "01-01-3000 00:00:00", "%m-%d-%Y %H:%M:%S"
        ),
    }


@pytest.fixture
def sample_point_data():
    """Create sample point data for testing."""
    return {
        "tags": {"city": "nyc", "state": "ny"},
        "fields": {"temp_f": 30.1, "humidity": 65.0},
        "measurement": "weather",
    }


def test_repr():
    """Test the repr method of Point class."""
    t = datetime.now(timezone.utc)
    t_str = t.isoformat()

    p = Point(
        time=t,
        measurement="m",
        tags={"a": "b", "c": "d"},
        fields={"my_field": 3.0},
    )
    s = (
        f"Point(time={t_str}, "
        f"measurement=m, "
        f"tags=a:b; c:d, "
        f"fields=my_field:3.0)"
    )

    assert repr(p) == s


def test_args_and_kwargs_valid():
    """Test valid Point creation with no args."""
    Point()  # Should be valid


@pytest.mark.parametrize(
    "args,kwargs,exception_type",
    [
        # Point with unnamed args should be invalid
        ((1,), {}, TypeError),
        (({"tk": "tv"},), {}, TypeError),
        (({"tk": 1},), {}, TypeError),
        (("my_measurement",), {}, TypeError),
        # Point with bad kwargs should be invalid
        ((), {"a": 1}, TypeError),
        (
            (),
            {
                "time": datetime.now(timezone.utc),
                "tags": {},
                "fields": {},
                "measurement": "",
                "other_field": {},
            },
            TypeError,
        ),
        # Point with bad types
        ((), {"time": 1}, ValueError),
        ((), {"measurement": 1}, ValueError),
    ],
)
def test_args_and_kwargs_invalid(args, kwargs, exception_type):
    """Test invalid Point creation scenarios."""
    with pytest.raises(exception_type):
        Point(*args, **kwargs)


@pytest.mark.parametrize(
    "validate_func,invalid_values,valid_values",
    [
        (
            validate_tags,
            [3, {1: "A"}, {"a": 1}],
            [{"a": "b"}, {"key": "value"}, {"city": "nyc", "state": "ny"}],
        ),
        (
            validate_fields,
            [3, {1: "A"}, {"a": "A"}],
            [{"a": 1}, {"temp": 70.5}, {"count": 10, "avg": 25.3}],
        ),
    ],
)
def test_validate_functions(validate_func, invalid_values, valid_values):
    """Test validate_tags and validate_fields functions."""
    # Test invalid values
    for invalid_value in invalid_values:
        with pytest.raises(ValueError):
            validate_func(invalid_value)

    # Test valid values
    for valid_value in valid_values:
        validate_func(valid_value)  # Should not raise


def test_time(test_times):
    """Test Point time attribute."""
    p = Point()
    t = test_times["past"]

    assert p.time == p._time
    p.time = t
    assert p.time == p._time == t

    valid_values = [
        test_times["now"],
        test_times["year_2000"],
        test_times["year_3000"],
    ]

    points = [
        Point(
            time=i,
            tags={"city": "nyc"},
            fields={"temp_f": 30.1},
        )
        for i in valid_values
    ]

    for point, time in zip(points, valid_values):
        assert point.time == time


def test_measurement():
    """Test Point measurement attribute."""
    p = Point()
    m = "m"

    assert p.measurement == p._measurement
    p.measurement = m
    assert p.measurement == p._measurement == m

    valid_values = ["", "_", "some_measurement"]

    points = [
        Point(
            measurement=i,
            fields={"num_restaurants": 60},
        )
        for i in valid_values
    ]

    for point, measurement in zip(points, valid_values):
        assert point.measurement == measurement


@pytest.fixture
def invalid_tag_values(test_times):
    """Invalid values for tags."""
    test_time = test_times["now"]
    return [
        123.22,
        True,
        test_time,
        {123: True},
        {True: True},
        {test_time: "all good"},
        {tuple((1, 2)): "ok"},
        {"key": {"a": "b"}},
        {"a": True},
        {"a": 123},  # Invalid for tags (should be string)
    ]


@pytest.fixture
def invalid_field_values(test_times):
    """Invalid values for fields."""
    test_time = test_times["now"]
    return [
        123.22,
        True,
        test_time,
        {123: True},
        {True: True},
        {test_time: "all good"},
        {tuple((1, 2)): "ok"},
        {"key": {"a": "b"}},
        {"a": True},  # Invalid for fields (should be numeric/None)
        # Note: {"a": 123} is valid for fields, so not included
    ]


def test_tags(test_times, invalid_tag_values):
    """Test Point tags attribute."""
    # Test invalid tags.
    for invalid_value in invalid_tag_values:
        with pytest.raises((ValueError, TypeError)):
            Point(
                time=test_times["now"],
                tags=invalid_value,
                fields={"num_restaurants": 10},
            )

    # Test valid tags.
    p = Point()
    tags = {"a": "b"}

    assert p.tags == p._tags == {}
    p.tags = tags
    assert p.tags == p._tags == tags

    valid_values = [
        {"key1": "value1"},
        {"key1": ""},
        {"key1": None},
        {"key2": "value2", "key3": "value3"},
    ]

    points = [
        Point(
            time=test_times["now"],
            tags=i,
            fields={"num_restaurants": 10},
        )
        for i in valid_values
    ]

    for point, tags in zip(points, valid_values):
        assert point.tags == tags


def test_fields(test_times, invalid_field_values):
    """Test Point fields attribute."""
    # Invalid fields.
    for invalid_value in invalid_field_values:
        with pytest.raises(ValueError):
            Point(
                time=test_times["now"],
                tags={"key1": "value1"},
                fields=invalid_value,
            )

    p = Point()
    fields = {"a": 1.0}

    assert p.fields == p._fields
    p.fields = fields
    assert p.fields == p._fields == fields

    valid_values = [
        {"key1": None},
        {"key2": 3},
        {"key3": 33333.3},
        {"key4": 33333.3, "key5": 3},
    ]

    points = [
        Point(
            time=test_times["now"],
            tags={"tag1": "value1"},
            fields=i,
        )
        for i in valid_values
    ]

    for point, fields in zip(points, valid_values):
        assert point.fields == fields


def test_points_are_equal(test_times):
    """Test the __eq__ method of Point class."""
    p1 = Point(
        time=test_times["now"],
        tags={"city": "nyc"},
        fields={"temp_f": 30.1},
    )

    p2 = Point(
        time=test_times["now"],
        tags={"city": "nyc"},
        fields={"temp_f": 30.1},
    )

    p3 = Point(
        time=test_times["now"],
        tags={"city": "los angeles"},
        fields={"temp_f": 70.1},
    )

    assert p1 == p2
    assert p1 != p3
    assert p2 != p3
    assert p1 != {}
    assert p2 != {}
    assert p3 != {}


@pytest.mark.parametrize(
    "point_data,expected_tuple",
    [
        (
            {
                "tags": {"city": "nyc"},
                "fields": {"temp_f": 30.1},
            },
            lambda time_str: (
                time_str,
                "_default",
                "_tag_city",
                "nyc",
                "_field_temp_f",
                "30.1",
            ),
        ),
        (
            {
                "measurement": "cities",
                "tags": {"city": "la"},
                "fields": {"temp_f": 75.1, "population": 15000000},
            },
            lambda time_str: (
                time_str,
                "cities",
                "_tag_city",
                "la",
                "_field_temp_f",
                "75.1",
                "_field_population",
                "15000000.0",
            ),
        ),
    ],
)
def test_serialize_point(test_times, point_data, expected_tuple):
    """Test serialization of a Point object."""
    time_str = test_times["now"].replace(tzinfo=None).isoformat()

    p = Point(time=test_times["now"], **point_data)
    p_tuple = p._serialize_to_list()

    assert p_tuple == expected_tuple(time_str)


@pytest.mark.parametrize(
    "tuple_data,expected_point_data",
    [
        (
            lambda time_str: (
                time_str,
                "_default",
                "_tag_city",
                "nyc",
                "_field_temp_f",
                "30.1",
            ),
            {
                "tags": {"city": "nyc"},
                "fields": {"temp_f": 30.1},
            },
        ),
        (
            lambda time_str: (
                time_str,
                "cities",
                "_tag_city",
                "la",
                "_field_temp_f",
                "75.1",
                "_field_population",
                "15000000",
            ),
            {
                "measurement": "cities",
                "tags": {"city": "la"},
                "fields": {"temp_f": 75.1, "population": 15000000},
            },
        ),
        (
            lambda time_str: (time_str, "m", "_field_a", "_none"),
            {
                "measurement": "m",
                "fields": {"a": None},
            },
        ),
    ],
)
def test_deserialize_valid_point(test_times, tuple_data, expected_point_data):
    """Test deserialization of a Point object."""
    time_str = test_times["now"].isoformat()

    p_tuple = tuple_data(time_str)
    expected_point = Point(time=test_times["now"], **expected_point_data)

    deserialized_point = Point()._deserialize_from_list(p_tuple)

    assert deserialized_point == expected_point


def test_deserialize_invalid_point():
    """Test deserialization of an invalid Point."""
    # Bad time value.
    bad_time = "ASDF"
    p_list = [
        str(bad_time),
        "_default",
        "_field_temp_f",
        "asdf",
    ]

    with pytest.raises(
        ValueError,
        match="Invalid isoformat string: 'ASDF'",
    ):
        Point()._deserialize_from_list(p_list)


@pytest.mark.parametrize(
    "test_data,description",
    [
        (
            {
                "point_data": {"fields": {"a": 0, "b": 0.0, "c": None}},
                "expected_serialized": lambda p: [
                    p._serialize_to_list()[3] == "0.0",
                    p._serialize_to_list()[5] == "0.0",
                    p._serialize_to_list()[7] == p._none_str,
                ],
                "expected_fields": {"a": 0, "b": 0.0, "c": None},
            },
            "zero values (issue 23)",
        ),
        (
            {
                "point_data": {"fields": {"a": None}, "tags": {"a": None}},
                "expected_serialized": lambda p: [
                    p._serialize_to_list()[3] == p._none_str,
                    p._serialize_to_list()[5] == p._none_str,
                ],
                "expected_fields": {"a": None},
                "expected_tags": {"a": None},
            },
            "None values",
        ),
        (
            {
                "point_data": {"tags": {"a": ""}},
                "expected_serialized": lambda p: [
                    p._serialize_to_list()[3] == ""
                ],
                "expected_tags": {"a": ""},
            },
            "empty string tag values",
        ),
    ],
)
def test_serialize_special_values(test_data, description):
    """Test (de)serialization of special values."""
    p = Point(**test_data["point_data"])
    s = p._serialize_to_list()

    # Check serialized values
    for assertion in test_data["expected_serialized"](p):
        assert assertion

    # Test round-trip deserialization
    new_p = Point()._deserialize_from_list(s)
    assert p == new_p

    # Check specific field/tag values if provided
    if "expected_fields" in test_data:
        for key, value in test_data["expected_fields"].items():
            assert new_p.fields[key] == value

    if "expected_tags" in test_data:
        for key, value in test_data["expected_tags"].items():
            assert new_p.tags[key] == value


@pytest.mark.parametrize(
    "point_data,prefix_attr,key_indices,expected_values",
    [
        (
            {"fields": {"a": 0, "b": 0.0, "c": None}},
            "_compact_field_key_prefix",
            (2, 4, 6),
            {"a": 0, "b": 0.0, "c": None},
        ),
        (
            {"tags": {"a": "aa", "b": "bb", "c": None}},
            "_compact_tag_key_prefix",
            (2, 4, 6),
            {"a": "aa", "b": "bb", "c": None},
        ),
    ],
)
def test_compact_keys(point_data, prefix_attr, key_indices, expected_values):
    """Test compact tag and field keys in CSV Storage."""
    p = Point(**point_data)
    s_compact = p._serialize_to_list(compact_key_prefixes=True)
    s_normal = p._serialize_to_list(compact_key_prefixes=False)

    # Check that compact prefixes are used
    prefix = getattr(p, prefix_attr)
    assert all(s_compact[i].startswith(prefix) for i in key_indices)

    # Test round-trip deserialization for both formats
    new_p_compact = Point()._deserialize_from_list(s_compact)
    new_p_normal = Point()._deserialize_from_list(s_normal)

    assert p == new_p_compact == new_p_normal

    # Check that all expected keys are present
    data_attr = "fields" if "fields" in point_data else "tags"
    assert all(
        key in getattr(new_p_compact, data_attr)
        for key in expected_values.keys()
    )

    # Check that values match expected
    for key, expected_value in expected_values.items():
        assert getattr(new_p_compact, data_attr)[key] == expected_value
