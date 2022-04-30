"""Tests for the tinyflux.point module."""
from datetime import datetime, timedelta
import pytest
from tinyflux.point import TagSet, FieldSet, Point


def test_repr():
    """Test the repr method of Point class."""
    t = datetime.utcnow()
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


def test_args_and_kwargs():
    """Test validation of args and kwargs for Point class."""
    # Point with no args should be valid.
    Point()

    # Point with unnamed args should be invalid.
    with pytest.raises(TypeError):
        Point(1)

    with pytest.raises(TypeError):
        Point({"tk": "tv"})

    with pytest.raises(TypeError):
        Point({"tk": 1})

    with pytest.raises(TypeError):
        Point("my_measurement")

    # Point with bad kwargs should be invalid.
    with pytest.raises(TypeError):
        Point(a=1)

    with pytest.raises(TypeError):
        Point(
            time=datetime.utcnow(),
            tags={},
            fields={},
            measurement="",
            other_field={},
        )

    # Point with bad time type.
    with pytest.raises(ValueError):
        Point(time=1)

    # Point with bad measurement type.
    with pytest.raises(ValueError):
        Point(measurement=1)


def test_tagset():
    """Test TagSet class and objects."""
    # Empty tag set.
    t = TagSet({})
    assert t == {}
    assert isinstance(t, TagSet)

    # Bad instatiation.
    with pytest.raises(ValueError, match="Tag set must be a mapping."):
        TagSet(3)

    with pytest.raises(
        ValueError, match="Tag set must contain only string keys."
    ):
        TagSet({1: "a"})

    with pytest.raises(
        ValueError, match="Tag set must contain only string values."
    ):
        TagSet({"a": 1})

    # Bad tag key setter.
    with pytest.raises(
        ValueError, match="Tag set must contain only string keys."
    ):
        t[1] = "a"

    # Bad tag value setter.
    with pytest.raises(
        ValueError, match="Tag set must contain only string values."
    ):
        t["a"] = 1


def test_fieldset():
    """Test FieldSet class and objects."""
    # Bad instatiation.
    with pytest.raises(ValueError, match="Field set must be a mapping."):
        FieldSet(3)

    with pytest.raises(
        ValueError, match="Field set must contain only string keys."
    ):
        FieldSet({1: "a"})

    with pytest.raises(
        ValueError, match="Field set must contain only numeric values."
    ):
        FieldSet({"a": "b"})

    # Bad tag key setter.
    with pytest.raises(
        ValueError, match="Field set must contain only string keys."
    ):
        FieldSet({})[1] = "a"

    # Bad tag value setter.
    with pytest.raises(
        ValueError, match="Field set must contain only numeric values."
    ):
        FieldSet({})["a"] = "b"

    # Invalid value types.
    with pytest.raises(ValueError, match="Field set must be a mapping."):
        FieldSet("a")

    with pytest.raises(ValueError, match="Field set must be a mapping."):
        FieldSet([])

    with pytest.raises(ValueError, match="Field set must be a mapping."):
        FieldSet(1)

    with pytest.raises(ValueError, match="Field set must be a mapping."):
        FieldSet(True)

    with pytest.raises(ValueError, match="Field set must be a mapping."):
        FieldSet({3})

    with pytest.raises(
        ValueError, match="Field set must contain only string keys."
    ):
        FieldSet({3: 1})

    with pytest.raises(
        ValueError, match="Field set must contain only string keys."
    ):
        FieldSet({True: 1})

    with pytest.raises(
        ValueError,
        match="Field set must contain only numeric values.",
    ):
        FieldSet({"a": "a"})

    with pytest.raises(
        ValueError,
        match="Field set must contain only numeric values.",
    ):
        FieldSet({"a": True})

    with pytest.raises(
        ValueError,
        match="Field set must contain only numeric values.",
    ):
        FieldSet({"a": {}})

    with pytest.raises(
        ValueError,
        match="Field set must contain only numeric values.",
    ):
        FieldSet({"a": []})

    # Field Set setter.
    fs = FieldSet({})
    fs["a"] = None
    assert fs["a"] == None
    fs["b"] = 1
    assert fs["b"] == 1


def test_time():
    """Test Point time attribute."""
    p = Point()
    t = datetime.utcnow() - timedelta(days=1)

    assert p.time == p._time
    p.time = t
    assert p.time == p._time == t

    valid_values = [
        datetime.utcnow(),
        datetime.strptime("01-01-2000 00:00:00", "%m-%d-%Y %H:%M:%S"),
        datetime.strptime("01-01-3000 00:00:00", "%m-%d-%Y %H:%M:%S"),
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


def test_tags():
    """Test Point tags attribute."""
    # Test invalid tags.
    invalid_values = [
        123.22,
        True,
        datetime.utcnow(),
        {123: True},
        {True: True},
        {datetime.utcnow(): "all good"},
        {tuple((1, 2)): "ok"},
        {"key": {"a": "b"}},
        {"a": True},
        {"a": 123},
    ]

    for i in invalid_values:
        with pytest.raises((ValueError, TypeError)):
            Point(
                time=datetime.utcnow(),
                tags=i,
                fields={"num_restaurants": 10},
            )

    # Test valid tags.
    p = Point()
    tags = {"a": "b"}

    assert p.tags == p._tags == {} == TagSet({})
    assert isinstance(p.tags, TagSet)
    p.tags = tags
    assert p.tags == p._tags == tags

    valid_values = [
        {
            "key1": "value1",
        },
        {"key2": "value2", "key3": "value3"},
    ]

    points = [
        Point(
            time=datetime.utcnow(),
            tags=i,
            fields={"num_restaurants": 10},
        )
        for i in valid_values
    ]

    for point, tags in zip(points, valid_values):
        assert point.tags == tags


def test_fields():
    """Test Point fields attribute."""
    # Invalid fields.
    invalid_values = [
        123.22,
        True,
        datetime.utcnow(),
        {123: True},
        {True: True},
        {datetime.utcnow(): "all good"},
        {tuple((1, 2)): "ok"},
        {"key": {"a": "b"}},
        {"a": True},
        {"a": 123},
    ]

    for i in invalid_values:
        with pytest.raises(ValueError):
            Point(
                time=datetime.utcnow(),
                tags={"key1", "value1"},
                fields=i,
            )

    # Empty field set.
    t = FieldSet({})
    assert t == {}
    assert isinstance(t, FieldSet)

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
            time=datetime.utcnow(),
            tags={"tag1": "value1"},
            fields=i,
        )
        for i in valid_values
    ]

    for point, fields in zip(points, valid_values):
        assert point.fields == fields


def test_points_are_equal():
    """Test the __eq__ method of Point class."""
    time_now = datetime.utcnow()

    p1 = Point(
        time=time_now,
        tags={"city": "nyc"},
        fields={"temp_f": 30.1},
    )

    p2 = Point(
        time=time_now,
        tags={"city": "nyc"},
        fields={"temp_f": 30.1},
    )

    p3 = Point(
        time=time_now,
        tags={"city": "los angeles"},
        fields={"temp_f": 70.1},
    )

    assert p1 == p2
    assert p1 != p3
    assert p2 != p3
    assert p1 != {}
    assert p2 != {}
    assert p3 != {}


def test_serialize_point():
    """Test serializaiton of a Point object."""
    time_now = datetime.utcnow()
    time_now_str = time_now.isoformat()

    p1 = Point(
        time=time_now,
        tags={"city": "nyc"},
        fields={"temp_f": 30.1},
    )

    p2 = Point(
        time=time_now,
        measurement="cities",
        tags={"city": "la"},
        fields={"temp_f": 75.1, "population": 15000000},
    )

    p_tuple1 = p1._serialize()
    p_tuple2 = p2._serialize()

    p_tuple_expected1 = (
        time_now_str,
        "_default",
        "_tag_city",
        "nyc",
        "_field_temp_f",
        30.1,
    )

    p_tuple_expected2 = (
        time_now_str,
        "cities",
        "_tag_city",
        "la",
        "_field_temp_f",
        75.1,
        "_field_population",
        15000000,
    )

    assert p_tuple1 == p_tuple_expected1
    assert p_tuple2 == p_tuple_expected2


def test_deserialize_valid_point():
    """Test deserialization of a Point object."""
    time_now = datetime.utcnow()
    time_now_str = time_now.isoformat()

    p_tuple = (
        time_now_str,
        "_default",
        "_tag_city",
        "nyc",
        "_field_temp_f",
        "30.1",
    )

    p1_expected = Point(
        time=time_now, tags={"city": "nyc"}, fields={"temp_f": 30.1}
    )

    p1 = Point()._deserialize(p_tuple)

    assert p1 == p1_expected

    p_tuple = (
        time_now_str,
        "cities",
        "_tag_city",
        "la",
        "_field_temp_f",
        "75.1",
        "_field_population",
        "15000000",
    )

    p2_expected = Point(
        time=time_now,
        measurement="cities",
        tags={"city": "la"},
        fields={"temp_f": 75.1, "population": 15000000},
    )

    p2 = Point()._deserialize(p_tuple)

    assert p2 == p2_expected

    p_tuple = (time_now_str, "m", "_field_a", "_none")

    p3_expected = Point(
        time=time_now,
        measurement="m",
        fields={"a": None},
    )

    p3 = Point()._deserialize(p_tuple)

    assert p3 == p3_expected


def test_deserialize_invalid_point():
    """Test deserialization of an invalid Point."""
    time_now = datetime.utcnow()
    time_now_str = time_now.isoformat()

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
        Point()._deserialize(p_list)

    # Bad value type.
    p_list = [
        time_now_str,
        "_default",
        "_field_temp_f",
        "asdf",
    ]

    with pytest.raises(ValueError, match="Invalid field value 'asdf'"):
        Point()._deserialize(p_list)

    p_list = [
        time_now_str,
        "_default",
        "_field_temp_f",
        "True",
    ]

    with pytest.raises(ValueError, match="Invalid field value 'True'"):
        Point()._deserialize(p_list)

    # Bad columns.
    p_list = [
        time_now_str,
        "_default",
        "_fieldzzz_temp_f",
        "75.1",
    ]

    with pytest.raises(
        RuntimeError, match="Unexpected schema for column '_fieldzzz_temp_f'"
    ):
        Point()._deserialize(p_list)

    p_list = [
        time_now_str,
        "_default",
        "_field_temp_f",
        "75.1",
        "bad_column",
        "1",
    ]

    with pytest.raises(
        RuntimeError, match="Unexpected schema for column 'bad_column'"
    ):
        Point()._deserialize(p_list)
