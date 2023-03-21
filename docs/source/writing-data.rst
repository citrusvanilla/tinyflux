Writing Data
============

The standard method for inserting a new data point is through the ``db.insert(...)`` method.  To insert more than one Point at the same time, use the ``db.insert_multiple([...])`` method, which accepts a ``list`` of points.  This might be useful when creating a TinyFlux database from a CSV of existing observations.

.. hint::

    To save space in text-based storage instances (including ``CSVStorage``), set the ``compact_key_prefixes`` argument to ``true`` in the ``.insert()`` and ``.insert_multiple()`` methods.  This will result in the tag and field keys having a shorter ``t_`` and ``f_`` prefix in front of them in the storage layer rather than the default ``__tag__`` and ``__field__`` prefixes.  Regardless of your choice, TinyFlux will handle Points with either prefix in the database.

.. note:: 

    **TinyFlux vs. TinyDB Alert!**
    
    In TinyDB there is a serious performance reason to use ``db.insert_multiple([...])`` over ``db.insert(...)`` as every write in TinyDB is preceeded by a full read of the data.  TinyFlux inserts are *append-only* and are **not** preceeded by a read.  Therefore, there is no significant *performance* reason to use ``db.insert_multiple([...])`` instead of ``db.insert(...)``.  If you are using TinyFlux to capture real-time data, you should insert points into TinyFlux as you see them, with ``db.insert(...)``.

Example:

>>> from tinyflux import Point
>>> p = Point(
...     measurement="air quality",
...     time=datetime.fromisoformat("2020-08-28T00:00:00-07:00"),
...     tags={"city": "LA"},
...     fields={"aqi": 112}
... )
>>> db.insert(p)

To recap, these are the two methods supporting the insertion of data.

+------------------------------------------------------------------+-----------------------------------------------------+
| **Methods**                                                                                                            |
+------------------------------------------------------------------+-----------------------------------------------------+
| ``db.insert(point, compact_key_prefixes=False)``                 | Insert one Point into the database.                 |
+------------------------------------------------------------------+-----------------------------------------------------+
| ``db.insert_multiple([point, ...], compact_key_prefixes=False)`` | Insert multiple Points into the database.           |
+------------------------------------------------------------------+-----------------------------------------------------+
