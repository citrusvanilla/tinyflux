
Querying Data
=============

TinyFlux's query syntax will be familiar to users of popular ORM tools.  It is similar to that of  TinyDB, but TinyFlux contains four different query types, one for each of a point's four attributes.

The query types are:

- ``TimeQuery`` for querying points by ``time``.
- ``MeasurementQuery`` for querying points by ``measurement``.
- ``TagQuery`` for querying points by ``tags``.
- ``FieldQuery`` for querying points by ``fields``.

For the remainder of this section, we will be illustrating query examples with the ``.search()`` method of a TinyFlux database.  This is the most common way to query TinyFlux, and the method accepts a query and returns a ``list`` of ``Point`` objects matching the query.  In addition, there are a handful of other database methods that take queries as argument and perform some sort of search.  See the :doc:`exploring-data` section for details.

.. note::

    ``.search()`` will return Points in sorted time order by default.  To return points in insertion order, pass the ``sorted=False`` argument, like: ``db.search(query, sorted=False)``.


Simple Queries
..............

Examples of the four basic query types are below:

Measurement Queries
^^^^^^^^^^^^^^^^^^^

To query for a specific measurement, the right-hand side of the ``MeasurementQuery`` should be a Python ``str``:

>>> from tinydb import MeasurementQuery
>>> Measurement = MeasurementQuery()
>>> db.search(Measurement == "city temperatures")

Tag Queries
^^^^^^^^^^^

To query for tags, the *tag key* of interest takes the form of a query attribute (following the ``.``), while the *tag value* forms the right-hand side.  An example to illustrate:

>>> from tinydb import TagQuery
>>> Tags = TagQuery()
>>> db.search(Tags.city == "Greenwich")

This will query the database for all points with the tag key of ``city`` mapping to the tag value of ``Greenwich``.

Field Queries
^^^^^^^^^^^^^

Similar to tags, to query for fields, the field key takes the form of a query attribute, while the field value forms the right-hand side:

>>> from tinydb import FieldQuery
>>> Fields = FieldQuery()
>>> db.search(Fields.high > 50.0)

This will query the database for all points with the field key of ``high`` exceeding the value of 50.0.

Some tag keys and field keys are not valid Python identifiers (for example, if the key contains whitespace), and can alternately be queried with string attributes:

>>> from tinydb import TagQuery
>>> Tags = TagQuery()
>>> db.search(Tags["country name"] == "United States of America")

Time Queries
^^^^^^^^^^^^

To query based on time, the "right-hand side" of the ``TimeQuery`` should be a timezone-aware ``datetime`` object:

>>> from tinydb import TimeQuery
>>> from datetime import datetime, timezone
>>> Time = TimeQuery()
>>> db.search(Time > datetime(2000, 1, 1, tzinfo=timezone.utc))

To query for a range of timestamps, it is most-performant to combine two ``TimeQuery`` instances with the ``&`` operator (for more details on compound queries, see :ref:`Compound Queries and Query Modifiers` below):

>>> q1 = Time > datetime(1990, 1, 1, tzinfo=timezone.utc)
>>> q2 = Time < datetime(2020, 1, 1, tzinfo=timezone.utc)
>>> db.search(q1 & q2)

.. note::

    Queries can be optimized for faster results.  See :doc:`tips` for details on optimizing queries.


Advanced Simple Queries
.......................

Some queries require transformations or comparisons that go beyond the basic operators like ``==``, ``<``, or ``>``. To this end, TinyFlux supports the following queries:


**.map(...) <--> Arbitrary Transform Functions for All Query Types**

The ``map()`` method will transform the tag/field value, which will be compared against the right-hand side value from the query.

>>> # Get all points with a even value for 'number_of_pedals'.
>>> def mod2(value):
...     return value % 2
>>> Field = FieldQuery()
>>> db.search(Field.number_of_pedals.map(mod2) == 0)

or:

>>> # Get all points with a measurement starting with the letter "a".
>>> def get_first_letter(value):
...     return value[0]
>>> Measurement = MeasurementQuery()
>>> db.search(Measurement.map(get_first_letter) == "a")

.. warning:: 

    Resist the urge to build your own time range query using the ``.map()`` query method.  This will result in slow queries.  Instead, use two ``TimeQuery`` instances combined with the ``&`` or ``|`` operator.


**.test(...) <--> Arbitrary Test Functions for All Query Types**

The ``test()`` method will transform and test the tag/field value for truthiness, with no right-hand side value necessary.

>>> # Get all points with a even value for 'number_of_pedals'.
>>> def is_even(value):
...     return value % 2 == 0
>>> Field = FieldQuery()
>>> db.search(Field.number_of_pedals.test(is_even))

or:

>>> # Get all points with a measurement starting with the letter "a".
>>> def starts_with_a(value):
...     return value.startswith("a")
>>> Measurement = MeasurementQuery()
>>> db.search(Measurement.test(starts_with_a))


**.exists() <--> Existence of Tag Key or Field Key**

This applies to ``TagQuery`` and ``FieldQuery`` only.

>>> Field, Tag = TagQuery(), FieldQuery()
>>> db.search(Tag.user_name.exists())
>>> db.search(Field.age.exists())


**.matches(...) and .search(...) <--> Regular Expression Queries for Measurements and Tag Values**

RegEx queries that apply to ``MeasurementQuery`` and ``TagQuery`` only.

>>> # Get all points with a user name containing "john", case-invariant.
>>> Tag = TagQuery()
>>> db.search(Tag.user_name.matches('.*john.*', flags=re.IGNORECASE))


Compound Queries and Query Modifiers
....................................

TinyFlux also allows supports compound queries through the use of logical operators.  This is particularly useful for time queries when a time range is needed.

>>> from tinydb import TimeQuery
>>> from datetime import datetime, timezone
>>> Time = TimeQuery()
>>> q1 = Time > datetime(1990, 1, 1, tzinfo=timezone.utc)
>>> q2 = Time < datetime(2020, 1, 1, tzinfo=timezone.utc)
>>> db.search(q1 & q2)

The three supported logical operators are **logical-and**, **logical-or**, and **logical-not**.

Logical AND ("&")
^^^^^^^^^^^^^^^^^

>>> # Logical AND:
>>> Time = TimeQuery()
>>> t1 = datetime(2010, 1, 1, tzinfo=timezone.utc)
>>> t2 = datetime(2020, 1, 1, tzinfo=timezone.utc)
>>> db.search((Time >= t1) & (Time < t2)) # Get all points in 2010's.

Logical OR ("|")
^^^^^^^^^^^^^^^^

>>> # Logical OR:
>>> db.search((Time < t1) | (Time > t2)) # Get all points outside 2010's.

Logical NOT ("~")
^^^^^^^^^^^^^^^^^

>>> # Negate a query:
>>> Tag = TagQuery()
>>> db.search(~(Tag.city == 'LA')) # Get all points whose city is not "LA".

.. hint::

    When using ``&`` or ``|``, make sure you wrap your queries on both sides with parentheses or Python will confuse the syntax.

    Also, when using negation (``~``) you'll have to wrap the query you want to negate in parentheses.

    While not aesthetically pleasing to the eye, the reason for these parenthesis is that Python's binary operators (``&``, ``|``, and ``~``) have a higher operator precedence than comparison operators (``==``, ``>``, etc.). For this reason, syntax like ``~User.name == 'John'`` is parsed by Python as ``(~User.name) == 'John'`` which will throw an exception. See the Python `docs on operator precedence
    <https://docs.python.org/3/reference/expressions.html#operator-precedence>`_ for details.

.. note::

    You **cannot** use ``and`` as a substitue for ``&``, ``or`` as a subsititue for ``|``, or ``not`` as a substitute for ``~``.  The ``and``, ``or``, and ``not`` keywords are reserved in Python and cannot be overridden, as the ``&``, ``|``, and ``~`` operators have been for TinyFlux queries.


To wrap, here are the queries and search operations we've learned:

+-------------------------------------------------+------------------------------------------------------------------+
| **Simple Queries**                                                                                                 |
+-------------------------------------------------+------------------------------------------------------------------+
| ``MeasurementQuery() == my_measurement``        | Match any Point with the measurement ``my_measurement``          |
+-------------------------------------------------+------------------------------------------------------------------+
| ``TimeQuery() < my_time_value``                 | Match any Point with a timestamp prior to ``my_time_value``      |
+-------------------------------------------------+------------------------------------------------------------------+
| ``TagQuery().my_tag_key == my_tag_value``       | Matches any Point with a tag key of ``my_tag_key`` mapping to    |
|                                                 | a tag value of ``my_tag_value``                                  |
+-------------------------------------------------+------------------------------------------------------------------+
| ``FieldQuery().my_field_key == my_field_value`` | Matches any Point with a field key of ``my_field_key`` mapping   |
|                                                 | to a field value of ``my_field_value``                           |
+-------------------------------------------------+------------------------------------------------------------------+
| **Advanced Simple Queries**                                                                                        |
+-------------------------------------------------+------------------------------------------------------------------+
| ``FieldQuery().my_field.exists()``              | Match any Point where a field called ``my_field`` exists         |
+-------------------------------------------------+------------------------------------------------------------------+
| ``FieldQuery().my_field.map()``                 | Transform and tag or field value for comparison to a             |
|                                                 | right-hand side value.                                           |
+-------------------------------------------------+------------------------------------------------------------------+
| ``FieldQuery().my_field.test(func, *args)``     | Matches any Point for which the function returns                 |
|                                                 | ``True``                                                         |
+-------------------------------------------------+------------------------------------------------------------------+
| ``FieldQuery().my_field.matches(regex)``        | Match any Point with the whole field matching the                |
|                                                 | regular expression                                               |
+-------------------------------------------------+------------------------------------------------------------------+
| ``FieldQuery().my_field.search(regex)``         | Match any Point with a substring of the field matching           |
|                                                 | the regular expression                                           |
+-------------------------------------------------+------------------------------------------------------------------+
| **Compound Queries and Query Modifiers**                                                                           |
+-------------------------------------------------+------------------------------------------------------------------+
| ``~(query)``                                    | Match Points that don't match the query                          |
+-------------------------------------------------+------------------------------------------------------------------+
| ``(query1) & (query2)``                         | Match Points that match both queries                             |
+-------------------------------------------------+------------------------------------------------------------------+
| ``(query1) | (query2)``                         | Match Points that match at least one of the queries              |
+-------------------------------------------------+------------------------------------------------------------------+
