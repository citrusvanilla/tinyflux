"""TinyFlux is a tiny, time-series database optimized for your happiness.

TinyDB stores time-series data as Points using a configurable storage
mechanism. It comes with a syntax for querying data and storing data in
multiple measurements.

TinyFlux was built on a fork of TinyDB, authored by Markus Siemens (email:
markus_m-siemens.de).

Author:
    Justin Fung (@citrusvanilla, citrusvanilla@gmail.com)

Usage:
    >>> from tinyflux import TinyFlux, Point, FieldQuery, TimeQuery
    >>> db = TinyFlux("my_tinyflux_db.csv")
    >>> p = Point(
    ...     measurement="california air quality",
    ...     time=datetime.fromisoformat("2020-01-01T00:00:00-08:00"),
    ...     tags={
    ...         "city": "Los Angeles",
    ...         "parameter": "PM2.5",
    ...     },
    ...     fields={"aqi": 112}
    ... )
    >>> db.insert(p)
    >>> q1 = TimeQuery() >= datetime.fromisoformat("2020-01-01T00:00:00-00:00")
    >>> q2 = FieldQuery().aqi > 100
    >>> hazardous_days_in_LA_2020 = db.search(q1 & q2)
"""
from .database import TinyFlux
from .point import Point
from .queries import TagQuery, FieldQuery, MeasurementQuery, TimeQuery

__all__ = [
    "TinyFlux",
    "Point",
    "TagQuery",
    "FieldQuery",
    "MeasurementQuery",
    "TimeQuery",
]
