Updating Points
===============

Though updating time series data tends to occur much less frequently than with other types of data, TinyFlux nonetheless supports the update of data with two methods.  Update by query with the ``update()`` method and update all with the ``update_all()`` method.  Updates are provided to both methods through the keyword arguments of ``measurement``, ``time``, ``tags``, or ``fields``.  The values for these arguments are either static values, or a ``Callable`` returning static values.  See below for examples.

.. note:: 

    If you are a developer, or are otherwise interested in how TinyFlux performs updates behind the scenes, see the :ref:`Design Principles` page.

To update individual points in TinyFlux, first provide a query to the ``update()`` method, followed by one or more attributes to update and their values as keyword arguments.  For example, to update the measurement names in the database for all points whose measurement value is "cities" to "US Metros", we use a static value to the ``measurement`` keyword argument:

>>> Measurement = MeasurementQuery()
>>> db.update(Measurement == "cities", measurement="US Metros")

Say we want update all timestamps for the measurement "US Metros" to be shifted backwards in time by one year, we would use a callable as the ``time`` keyword argument instead of a static value.

>>> from datetime import timedelta
>>> Measurement = MeasurementQuery()
>>> db.update(Measurement == "US Metros", time=lambda x: x - timedelta(days=365))

Now we want to change all instances of "CA" to "California" in a point's tag set for the "US Metros" measurement.

>>> Measurement = MeasurementQuery()
>>> def california_updater(tags):
...     if "state" in tags and tags["state"] == "CA":
...         return {**tags, "state": "California"}
...     else:
...         return tags
>>> db.update(Measurement == "US Metros", tags=california_updater)

Field updates occur much the same way as tags.  To update all items in the database, we use ``update_all()``.  For example, to convert all temperatures from Fahrenheit to Celcius if the field ``temp`` exists:

>>> def fahrenheit_to_celcius(fields):
...     if "temp" in fields:
...         temp_f = fields["temp"]
...         temp_c =  (temp_f - 32.0) * (5/9)
...         return {**fields, "temp": temp_c}
...     else:
...         return fields
>>> db.update_all(fields=fahrenheit_to_celcius)

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
