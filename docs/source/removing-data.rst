Removing Points
===============

TinyFlux supports the removal of data with two methods.  To remove by query, the ``remove()`` method is provided, and to remove all, use the ``remove_all()`` method.  See below for examples.

.. note:: 

    If you are a developer, or are otherwise interested in how TinyFlux performs deletes behind the scenes, see the :doc:`design-principles` page.

The following will remove all points with the measurement value of "US Metros":

>>> Measurement = MeasurementQuery()
>>> db.remove(Measurement == "US Metros")

The following is an example of a manual time-based eviction.

>>> from datetime import datetime, timedelta, timezone
>>> Time = TimeQuery()
>>> t = datetime.now(timezone.utc) - timedelta(days=7)
>>> # Remove all points older that seven days.
>>> db.remove(Time < t)

To remove everything in the database , invoke ``remove_all()``:

>>> db.remove_all()

.. warning:: 

    Like all other operations in TinyFlux, you cannot roll back the actions of ``remove()`` or ``remove_all()``.  There is no confirmation step, no access-control mechanism that prevents non-admins from performing this action, nor are there automatic snapshots stored anywhere.  If you need these kinds of features, TinyFlux is not for you.


to recap, these are the two methods supporting the removal of data.

+------------------------+-----------------------------------------------+
| **Methods**                                                            |
+------------------------+-----------------------------------------------+
| ``db.remove(query)``   | Remove any point matching the input query.    |
+------------------------+-----------------------------------------------+
| ``db.remove_all()``    | Remove all points.                            |
+------------------------+-----------------------------------------------+
