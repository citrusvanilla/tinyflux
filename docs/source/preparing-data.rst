Preparing Data
==============

Before inserting data into TinyFlux, data must be cast into specific types of objects known as a "Points".  Here's an example:

>>> from tinyflux import Point
>>> from datetime import datetime, timezone
>>> p = Point(
...     measurement="city temperatures",
...     time=datetime(2022, 1, 1, tzinfo=timezone.utc),
...     tags={"city": "Greenwich", "country": "England"},
...     fields={"high": 52.0, "low": 41.0}
... )

This term "Point" comes from InfluxDB. A well-formed Point consists of four attributes:

- ``measurement``: Known as a "table" in relational databases, its value type is ``str``.
- ``time``: The timestamp of the observation, its value is a Python ``datetime`` object that should be "timezone aware".
- ``tags``: Text attributes of the observation as a Python ``dict`` of ``str|str`` key value pairs.
- ``fields``: Numeric attributes of the observation as a Python ``dict`` of ``str|int`` or ``str|float`` key value pairs.

None of the four attributes is required during initialization; you can also initialize an empty Point like the following:

>>> from tinyflux import Point
>>> Point()
Point(time=None, measurement=_default)

You'll notice that the ``time`` attribute is ``None``, and the ``measurement`` attribute has taken the value of ``_default``.  The point also has no tags or fields.  Tags and fields are not required, but from a user's perspective, such a data point has little meaning.

.. note::

    Points that do not have ``time`` values take on timestamps *when they are inserted into TinyFlux, not when they are created*.  If you want `time` to reflect the time of creation, set time like: ``time=datetime.now(timezone.utc)``.

A default ``measurement`` is assigned to Points that are initialized without one.

Tags are string/string key value pairs.  The reason for having separate attributes for  ``tags`` and ``fields`` in TinyFlux (and in InfluxDB) is twofold: It enforces consistency of types and data on the user's side, and it allows the database to efficiently index on tags, which are attributes with low cardinality (compared to fields, which tend to have much higher variation across values).

.. note::

    While both TinyDB and TinyFlux are "schemaless", TinyFlux does not support complex types as values.  If you want to store documents, which are often collections rather than primitive types, take a look at TinyDB.

.. hint::
    
    TinyFlux will raise a ``ValueError`` if you try to initialize a ``Point`` with incorrect types, so you can be sure you are not inserting malformed data into the database.

