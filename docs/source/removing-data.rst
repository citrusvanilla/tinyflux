Removing Points
===============

Similar to updates, removal of data in time series datasets tends to occur much less frequently than with other types of data.  Like updates though, TinyFlux supports the removal of data with two methods.  You can remove by query with the ``remove()`` method and remove all with the ``remove_all()`` method.  See below for examples.

.. note:: 

    If you are a developer, or are otherwise interested in how TinyFlux performs deletes behind the scenes, see the :doc:`design-principles` page.

For the first example. lets remove all points with the measurement value of "US Metros":

>>> Measurement = MeasurementQuery()
>>> db.remove(Measurement == "US Metros")

As another example, we could perform manual time-based eviction by deleting points older than say, seven days:

>>> from datetime import datetime, timedelta, timezone
>>> Time = TimeQuery()
>>> t = datetime.now(timezone.utc) - timedelta(days=7)
>>> db.remove(Time < t)

Now, if for some reason you want to remove everything in the database and start fresh, simply invoke ``remove_all()``:

>>> db.remove_all()

.. warning:: 

    Like all other operations in TinyFlux, you cannot roll back the actions of ``remove()`` or ``remove_all()``.  There is no confirmation step, no access-control mechanism that prevents non-admins from performing this action, nor are there automatic snapshots stored anywhere.  If you need these kinds of features, TinyFlux is not for you.


to recap, these are the two methods supporting the removal of data.

+------------------------------------------+-----------------------------------------------------+
| **Methods**                                                                                    |
+------------------------------------------+-----------------------------------------------------+
| ``db.remove(query)``                     | Remove any point matching the input query.          |
+------------------------------------------+-----------------------------------------------------+
| ``db.remove_all()``                      | Remove all points.                                  |
+------------------------------------------+-----------------------------------------------------+
