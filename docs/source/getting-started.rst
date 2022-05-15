Getting Started
===============

Initialize a new TinyFlux database (or connect to an existing file store) with the following:

>>> from tinyflux import TinyFlux
>>> db = TinyFlux('db.csv')

``db`` is now a reference to the TinyFlux database that stores its data in a file called ``db.csv``.

An individual instance of data in a TinyFlux database is known as a "Point".  In a traditional relational database, this would called called a "row", and in a document-oriented database it is called a "document".  A TinyFlux ``Point`` is a convenient object for storing its four main attributes:

+-----------------+----------------------------------------------------------+------------------------------------------------------+
| **Attribute**   | **Python Type**                                          | **Example**                                          |
+-----------------+----------------------------------------------------------+------------------------------------------------------+
| ``measurement`` | ``str``                                                  | ``"california air quality"``                         |
+-----------------+----------------------------------------------------------+------------------------------------------------------+
| ``time``        | ``datetime``                                             | ``datetime.now(timezone.utc)``                       |
+-----------------+----------------------------------------------------------+------------------------------------------------------+
| ``tags``        | ``Dict`` of ``str`` keys and ``str`` values              | ``{"city": "Los Angeles", "parameter": "PM2.5"}``    |
+-----------------+----------------------------------------------------------+------------------------------------------------------+
| ``fields``      | ``Dict`` of ``str`` keys and ``float`` or ``int`` values | ``{"aqi": 112.0}``                                   |
+-----------------+----------------------------------------------------------+------------------------------------------------------+

In keeping with the analogy of a traditional RDMS, a ``measurement`` is like a table.

``time`` is a field with the requirement that it is a ``datetime`` type, ``tags`` is a collection of string attributes, and ``fields`` is a collection of numeric attributes.  TinyFlux is "schemaless", so tags and fields can be added/removed to any Point.

To make a Point, import the Point definition and annotate the Point with the desired attributes.  If ``measurement`` is not defined, it takes the default table name of ``_default``.

>>> from tinyflux import Point
>>> p1 = Point(
...     time=datetime.fromisoformat("2020-08-28T00:00:00-07:00"),
...     tags={"city": "LA"},
...     fields={"aqi": 112}
... )
>>> p2 = Point(
...     time=datetime.fromisoformat("2020-12-05T00:00:00-08:00"),
...     tags={"city": "SF"},
...     fields={"aqi": 128}
... )

To write to TinyFlux, simply:

>>> db.insert(p1)
>>> db.insert(p2)

All points can be retriebved from the database with the following:

>>> db.all()
[Point(time=2020-01-01T00:08:00-00:00, measurement=_default, tags=city:LA, fields=aqi:112), Point(time=2020-12-05T00:08:00-00:00, measurement=_default, tags=city:SF, fields=aqi:128)]

.. note:: TinyFlux will convert all time to UTC. Read more about it here: :doc:`time`.

TinyFlux also allows iteration over stored Points:

>>> for point in db:
>>>     print(point)
Point(time=2020-08-28T00:07:00-00:00, measurement=_default, tags=city:LA, fields=aqi:112)
Point(time=2020-12-05T00:08:00-00:00, measurement=_default, tags=city:SF, fields=aqi:128)

To query for Points, there are four query types- one for each of a Point's four attributes.

>>> from tinyflux import FieldQuery, MeasurementQuery, TagQuery, TimeQuery
>>> Time = TimeQuery()
>>> db.search(Time < datetime.fromisoformat("2020-11-00T00:00:00-08:00"))
[Point(time=2020-08-28T00:07:00-00:00, measurement=_default, tags=city:LA, fields=aqi:112)]
>>> Field = FieldQuery()
>>> db.search(Field.aqi > 120)
[Point(time=2020-12-05T00:08:00-00:00, measurement=_default, tags=city:SF, fields=aqi:128)]
>>> Tag = TagQuery()
>>> db.search(Tag.city == "LA")
[Point(time=2020-08-28T00:07:00-00:00, measurement=_default, tags=city:LA, fields=aqi:112)]
>>> Measurement = MeasurementQuery()
>>> db.count(Measurement == "_default")
2

Points can also be updated.

>>> # Update the ``aqi`` field of the Los Angeles point.
>>> db.update(tag.city == "LA", tags={"aqi": 118})
>>> for point in db:
>>>     print(point)
Point(time=2020-08-28T00:07:00-00:00, measurement=_default, tags=city:LA, fields=aqi:118)
Point(time=2020-12-05T00:08:00-00:00, measurement=_default, tags=city:SF, fields=aqi:128)

Points can also be removed.

>>> db.remove(tag.city == "SF")
1
>>> db.all()
[Point(time=2020-01-01T00:08:00-00:00, measurement=_default, tags=city:LA, fields=aqi:112)]

Here is the basic syntax covered in this section:

+-------------------------------+---------------------------------------------------------------+
| **Initialize a new TinyFlux Database**                                                        |
+-------------------------------+---------------------------------------------------------------+
| ``db = TinyFlux("my_db.csv")``| Initialize or connect to existing with ``TinyFlux()``         |
+-------------------------------+---------------------------------------------------------------+
| **Creating New Points**                                                                       |
+-------------------------------+---------------------------------------------------------------+
| ``Point(...)``                | Initialize a new point.                                       |
+-------------------------------+---------------------------------------------------------------+
| **Inserting Points Into the Database**                                                        |
+-------------------------------+---------------------------------------------------------------+
| ``db.insert()``               | Insert a point.                                               |
+-------------------------------+---------------------------------------------------------------+
| **Retrieving Points**                                                                         |
+-------------------------------+---------------------------------------------------------------+
| ``db.all()``                  | Get all points                                                |
+-------------------------------+---------------------------------------------------------------+
| ``iter(db)``                  | Iterate over all points                                       |
+-------------------------------+---------------------------------------------------------------+
| ``db.search(query)``          | Get a list of points matching the query                       |
+-------------------------------+---------------------------------------------------------------+
| ``db.count(query)``           | Count the number of points matching the query                 |
+-------------------------------+---------------------------------------------------------------+
| **Updating Points**                                                                           |
+-------------------------------+---------------------------------------------------------------+
| ``db.update(query, ...)``     | Update all points matching the query                          |
+-------------------------------+---------------------------------------------------------------+
| **Removing Points**                                                                           |
+-------------------------------+---------------------------------------------------------------+
| ``db.remove(query)``          | Remove all points matching the query                          |
+-------------------------------+---------------------------------------------------------------+
| ``db.remove_all()``           | Remove all points                                             |
+-------------------------------+---------------------------------------------------------------+
| **Querying TinyFlux**                                                                         |
+-------------------------------+---------------------------------------------------------------+
| ``TimeQuery()``               | Create a new time query object                                |
+-------------------------------+---------------------------------------------------------------+
| ``FieldQuery().f_key == 2``   | Match any point that has a field ``f_key`` with value         |
|                               | ``== 2`` (also possible: ``!=``, ``>``, ``>=``, ``<``, ``<=``)|
+-------------------------------+---------------------------------------------------------------+

To continue with the introduction to TinyFlux, proceed to the next section, :doc:`preparing-data`.

