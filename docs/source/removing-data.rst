Removing Points
===============

TinyFlux supports the removal of points with two methods.  To remove by query, the ``remove()`` method is provided, and to remove all, use the ``remove_all()`` method.  See below for examples.

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


To recap, these are the two methods supporting the removal of data.

+------------------------+-----------------------------------------------+
| **Methods**                                                            |
+------------------------+-----------------------------------------------+
| ``db.remove(query)``   | Remove any point matching the input query.    |
+------------------------+-----------------------------------------------+
| ``db.remove_all()``    | Remove all points.                            |
+------------------------+-----------------------------------------------+

Removing Tags and Fields
========================

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