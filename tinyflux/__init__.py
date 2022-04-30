"""TinyFlux is a tiny, time-series database optimized for your happiness.

TinyDB stores time-series data as Points using a configurable storage
mechanism. It comes with a syntax for querying data and storing data in
multiple measurements.

TinyFlux was built on a fork of TinyDB, authored by Markus Siemens (email:
markus_m-siemens.de).

Author:
    Justin Fung (@citrusvanilla, citrusvanilla@gmail.com)

Usage:
    >>> from tinyflux import TinyFlux, Point, FieldQuery
    >>> db = TinyFlux("my_new_tinyflux_db.csv")
    >>> p = Point(
    ...     time=datetime.utcnow(),
    ...     measurement="cities",
    ...     tags={"city": "Los Angeles"},
    ...     fields={"temp_f": 70.0}
    ... )
    >>> db.insert(p)
    >>> rst = db.search(FieldQuery().temp_f > 32.0)
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
