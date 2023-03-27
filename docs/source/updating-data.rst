Updating Points
===============

Though updating time series data tends to occur much less frequently than with other types of data, TinyFlux nonetheless supports the updating of data with two methods: 1. Update by query with the ``update()`` method, and 2. Update all points with the ``update_all()`` method.  ``measurement``, ``time``, ``tags``, and/or ``fields`` are updated on an individual basis through the associated keyword arguments to these two methods.  The values for these arguments are either static values (like a string, float, integer, or boolean), or a ``Callable`` returning static values.  See below for examples.

.. note:: 

    If you are a developer, or are otherwise interested in how TinyFlux performs updates behind the scenes, see the :doc:`design-principles` page.

To update individual points in TinyFlux, first provide a query to the ``update()`` method, followed by one or more attributes to update and their values as keyword arguments.  For example, to update the measurement names in the database for all points whose measurement value is "cities" to "US Metros", use a static value to the ``measurement`` keyword argument:

>>> Measurement = MeasurementQuery()
>>> db.update(Measurement == "cities", measurement="US Metros")

To update all timestamps for the measurement "US Metros" to be shifted backwards in time by one year, use a callable as the ``time`` keyword argument instead of a static value:

>>> from datetime import timedelta
>>> Measurement = MeasurementQuery()
>>> db.update(Measurement == "US Metros", time=lambda x: x - timedelta(days=365))

To change all instances of "CA" to "California" in a point's tag set for the "US Metros" measurement:

>>> Measurement = MeasurementQuery()
>>> def california_updater(tags):
...     if "state" in tags and tags["state"] == "CA":
...         return {**tags, "state": "California"}
...     else:
...         return tags
>>> db.update(Measurement == "US Metros", tags=california_updater)

Field updates occur much the same way as tags.  To update all items in the database, use ``update_all()``.  For example, to convert all temperatures from Fahrenheit to Celcius if the field ``temp`` exists:

>>> def fahrenheit_to_celcius(fields):
...     if "temp" in fields:
...         temp_f = fields["temp"]
...         temp_c =  (temp_f - 32.0) * (5/9)
...         return {**fields, "temp": temp_c}
...     else:
...         return fields
>>> db.update_all(fields=fahrenheit_to_celcius)

.. note:: 

    Updating data with `.update()` or `.update_all()` through the `tags` or `fields` arguments will not remove tags or fields, even if they are not returned when using a Callable as the updater.  This is consistent with the Python `dict API <https://docs.python.org/3/library/stdtypes.html#dict.update>`_, in which keys can be overwritten, but not deleted.  To remove tags and fields completely, see :ref:`Removing Tags and Fields with Update` below.

.. warning:: 

    Like all other operations in TinyFlux, you cannot roll back the actions of ``update()`` or ``update_all()``.  There is no confirmation step, no access-control mechanism that prevents non-admins from performing this action, nor are there automatic snapshots stored anywhere.  If you need these kinds of features, TinyFlux is not for you.

to recap, these are the two methods supporting the updating of data.

+------------------------------------------+-----------------------------------------------------+
| **Methods**                                                                                    |
+------------------------------------------+-----------------------------------------------------+
| ``db.update(query, ...)``                | Update any point matching the input query.          |
+------------------------------------------+-----------------------------------------------------+
| ``db.update_all(...)``                   | Update all points.                                  |
+------------------------------------------+-----------------------------------------------------+

Removing Tags and Fields with Update
------------------------------------

TinyFlux supports the removal of individual tag and field key/values through the `unset_tags` and `unset_fields` arguments to `.update()` and `.update_all()`.  The values can be either individual strings, or lists of strings.  See below for examples.

The following will remove all tags with the key of "city" from the database:

>>> db.update_all(unset_tags="city")

The following will remove all tags with the keys of "state" and "country" from the database:

>>> db.update_all(unset_tags=["state", "country"])

The following will remove all tags with the key of "temperature" from all Points in the "bedroom" measurement:

>>> db.update(MeasurementQuery() == "bedroom", unset_tags=["temperature"])

.. warning:: 

    Like all other operations in TinyFlux, you cannot roll back the actions of ``update()`` or ``update_all()``.  There is no confirmation step, no access-control mechanism that prevents non-admins from performing this action, nor are there automatic snapshots stored anywhere.  If you need these kinds of features, TinyFlux is not for you.


To recap, these are the two methods supporting the removal of individual tags and fields from points.

+------------------------------------------------------------+------------------------------------------------------------+
| **Methods**                                                                                                             |
+------------------------------------------------------------+------------------------------------------------------------+
| ``db.update(query, unset_tags=..., unset_fields=...)``     | Remove the tags and fields from points matching the query. |
+------------------------------------------------------------+------------------------------------------------------------+
| ``db.update_all(query, unset_tags=..., unset_fields=...)`` | Remove specified tags and fields from all points.          |
+------------------------------------------------------------+------------------------------------------------------------+