Timezones in TinyFlux
---------------------

Timestamps going in and out of TinyFlux are of the Python ``datetime`` type.  At the file storage layer, TinyFlux stores these timestamps as ISO formatted strings in UTC.  For seasoned Python users, this will be a familiar practice, as they will already be using timezone aware datetime objects in all cases and used to converting to-and-from UTC.

.. hint::

    If you aren't already using timezone-aware datetime objects, there is no better time to start than now.

.. hint::

    TLDR: All timestamps should be input as timezone-aware datetime objects in the UTC timezone.  If you need to keep information about the local timezone of the observation, store it as a tag.  Skip to example 5 below for proper initialization.

To illustrate the way time is handled in TinyFlux, below are the five ways time could potentially be initialized by a user.  The fifth and final example is "best practice":

1.  ``time`` is not set by the user when the Point is initialized so its default value is ``None``.  AFTER it is inserted into TinyFlux, it is assigned a UTC timestamp corresponding to the time of insertion.

    >>> from tinyflux import Point, TinyFlux
    >>> db = TinyFlux("my_db.csv") # an empty db
    >>> p = Point()
    >>> p.time is None
    True
    >>> db.insert(p)
    >>> p.time
    datetime.datetime(2021, 10, 30, 13, 53, 552872, tzinfo=datetime.timezone.utc)

2.  ``time`` is set with a value, but it is not a ``datetime`` object.  TinyFlux raises an exception.

    >>> Point(time="2022-01-01")
    ValueError: Time must be datetime object.

3.  ``time`` is set with a ``datetime`` object that is "timezone-naive".  TinyFlux considers this time to be local to the timezone of the computer that is running TinyFlux and will convert this time to UTC using the ``astimezone`` attribute of the ``datetime`` module upon insertion.  This will lead to confusion down the road if TinyFlux is running on a remote computer, or the user was annotating data for points corresponding to places in other timezones.

    >>> from datetime import datetime
    >>> # Example: Our computer is in Californa, but we are working with a dataset of
    >>> #          air quality measurements for Beijing, China.
    >>> # Here, AQI was measured at 1pm local time in Beijing on Aug 28, 2021.
    >>> p = Point(
    ...     time=datetime(2021, 8, 28, 13, 0), # 1pm, datetime-naive
    ...     tags={"city": "beijing"},
    ...     fields={"aqi": 118}
    ... )
    >>> p.time
    datetime.datetime(2021, 8, 28, 13, 0)
    >>> # Insert the point into the database.
    >>> db.insert(p)
    >>> # The point is cast to UTC, assuming the time was local to California, not Beijing.
    >>> p.time
    datetime.datetime(2021, 8, 28, 20, 0, tzinfo=datetime.timezone.utc)


4.  ``time`` is set with a ``datetime`` object that is timezone-aware but the timezone is not UTC- TinyFlux casts the time to UTC for internal storage and retrieval and the original timezone is lost (it is up to the user to cast the timezone again after retrieval).

    >>> from tinyflux import Point, TinyFlux
    >>> from datetime import datetime
    >>> from zoneinfo import ZoneInfo
    >>> db = TinyFlux("my_db.csv") # an empty db
    >>> la_point = Point(
    ...     time=datetime(2000, 1, 1, tzinfo=ZoneInfo("US/Pacific")),
    ...     tags={"city": "Los Angeles"}
    ...     fields={"temp_f": 54.0}
    ... )
    >>> ny_point = Point(
    ...     time=datetime(2000, 1, 1, tzinfo=ZoneInfo("US/Eastern")),
    ...     tags={"city": "New York City"}
    ...     fields={"temp_f": 15.0}
    ... )
    >>> db.insert_multiple([la_point, ny_point])
    >>> # Notice the time attributes no longer carry the timezone information:
    >>> la_point.time
    datetime.datetime(2000, 1, 1, 8, 0, tzinfo=datetime.timezone.utc)
    >>> ny_point.time
    datetime.datetime(2000, 1, 1, 5, 0, tzinfo=datetime.timezone.utc)

    .. hint::

        If you need to keep the original, non-UTC timezone along with the dataset, consider adding a ``tag`` to your point indicating the timezone, for easier conversion after retrieval.  TinyFlux will not assume nor attempt to store the timezone of your data for you.

5.  ``time`` is set with a ``datetime`` object that is timezone-aware and the timezone is UTC.  This is the easiest way to handle time.  If needed, infomation about the timezone is stored in a tag.

    >>> from datetime import datetime, timezone
    >>> from tinyflux import TinyFlux, Point
    >>> from zoneinfo import ZoneInfo
    >>> # Time now is 10am in Los Angeles, which is 6pm UTC:
    >>> t = datetime.now(timezone.utc)
    >>> t
    datetime.datetime(2022, 11, 9, 18, 0, 0, tzinfo=datetime.timezone.utc)
    >>> # Store the time in UTC, but keep the timezone as a tag for later use.
    >>> p = Point(
    ...     time=t,
    ...     tags={"room": "bedroom", "timezone": "America/Los_Angeles"},
    ...     fields={"temp": 72.0}
    ... )
    >>> # Time is still UTC:
    >>> p.time
    datetime.datetime(2022, 11, 9, 18, 0, 0, tzinfo=datetime.timezone.utc)
    >>> # To cast back to local time in Los Angeles:
    >>> la_timezone = ZoneInfo(p.tags["timezone"])
    >>> p.time.astimezone(la_timezone)
    datetime.datetime(2022, 11, 9, 10, 0, tzinfo=zoneinfo.ZoneInfo(key='America/Los_Angeles'))