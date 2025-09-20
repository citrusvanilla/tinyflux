Writing Data
============

The standard method for inserting a new data point is through the ``db.insert(...)`` method.  To insert more than one Point at the same time, use the ``db.insert_multiple([...])`` method, which accepts an iterable of points (including generators and iterators).  This method processes points in configurable batches for optimal performance and memory usage.

.. hint::

    To save space in text-based storage instances (including ``CSVStorage``), set the ``compact_key_prefixes`` argument to ``true`` in the ``.insert()`` and ``.insert_multiple()`` methods.  This will result in the tag and field keys having a shorter ``t_`` and ``f_`` prefix in front of them in the storage layer rather than the default ``__tag__`` and ``__field__`` prefixes.  Regardless of your choice, TinyFlux will handle Points with either prefix in the database.

.. note:: 

    **Performance Considerations**
    
    Starting in v1.1.0, ``db.insert_multiple([...])`` offers significant performance advantages over multiple ``db.insert(...)`` calls when inserting large datasets. The method processes points in batches (default: 1,000 points per batch), reducing the number of fsync operations and improving throughput.
    
    For real-time data capture where points arrive individually, continue using ``db.insert(...)`` for immediate persistence. For bulk data loading from CSVs, APIs, or other batch sources, use ``db.insert_multiple([...])`` for optimal performance.

.. tip::

    **Tuning Batch Size for Performance**
    
    The ``batch_size`` parameter controls how many points are processed together before writing to storage:
    
    - **Larger batch sizes** = Fewer fsync operations = Better performance, but higher memory usage
    - **Smaller batch sizes** = More frequent writes = Lower memory usage, but potentially slower
    - **Default (1,000)** provides a good balance for most use cases
    
    Examples:
    
    - For memory-constrained environments: ``db.insert_multiple(points, batch_size=100)``
    - For maximum performance with large datasets: ``db.insert_multiple(points, batch_size=10000)``
    - For generators/iterators: The method automatically handles memory efficiently regardless of total size

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

+-----------------------------------------------------------------------------+-----------------------------------------------------+
| **Methods**                                                                 | **Description**                                     |
+=============================================================================+=====================================================+
| ``db.insert(point, compact_key_prefixes=False)``                            | Insert one Point into the database.                 |
+-----------------------------------------------------------------------------+-----------------------------------------------------+
| ``db.insert_multiple(points, compact_key_prefixes=False, batch_size=1000)`` | Insert multiple Points with configurable batching.  |
+-----------------------------------------------------------------------------+-----------------------------------------------------+
