Tips for TinyFlux
=================

Below are some tips to get the most out of TinyFlux.


Saving Space
^^^^^^^^^^^^

If you are using a text-based storage layer (such as the default ``CSVStorage``) keep in mind that every character requires usually one (but up to four) bytes of memory for storage in a UTF-8 encoding.  To save space, here are a few tips:

• Keep measurement names, tag keys, and field keys short and concise.
• Precision matters!  Even more so with text-backed storage.  ``1.0000`` requires twice as much space to store compared to ``1.0``, and 5x more space than ``1``.
• When inserting points into TinyFlux, make sure to set the ``compact_key_prefixes`` option to ``True`` (e.g. ``db.insert(my_point, compact_key_prefixes=True)``).  This saves three bytes per tag key/value pair and five bytes per field key/value pair.

If your dataset is approaching 1 GB in size, keep reading.


Dealing with Growing Datasets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As concurrency is not a feature of TinyFlux, a growing database will incur increases in query and index-building times.  When queries start to slow down a workflow, it might be time to "shard" or de-normalize the data, or simply upgrade to a database server like InfluxDB.

For example, if a TinyFlux database currently holds Points for two separate measurements, consider making two separate databases, one for each measurement:

>>> from tinyflux import TinyFlux, Point, MeasurementQuery
>>> from datetime import datetime, timedelta, timezone
>>> db = TinyFlux("my_big_db.csv") # a growing db with two measurements
>>> db.count(MeasurementQuery() == "measurement_1")
70000
>>> db.count(MeasurementQuery() == "measurement_2")
85000
>>> new_db = TinyFlux("my_new_single_measurement_db.csv") # a new empty db
>>> for point in db:
>>>     if point.measurement == "measurement_2":
>>>         new_db.insert(point)
>>> db.remove(MeasurementQuery() == "measurement_2")
85000
>>> len(db)
70000
>>> len(new_db)
85000

.. hint::
    
    When queries and indexes slow down a workflow, consider creating separate databases.  Or, just migrate to InfluxDB.


Optimizing Queries
^^^^^^^^^^^^^^^^^^

Unlike TinyDB, TinyFlux never pulls in the entirety of its data into memory (unless the ``.all()`` method is called).  This has the benefit of reducing the memory footprint of the database, but means that database operations are usually I/O bound.  By using an index, TinyFlux is able to construct a matching set of items from the storage layer without actually reading any of those items.  For database operations that return Points, TinyFlux iterates over the storage, collects the items that belong in the set, deserializes them, and finally returns them to the caller.

This ultimately means that the smaller the set of matches, the less I/O TinyFlux must perform.

.. hint::
    
    Queries that return smaller sets of matches perform best.

.. warning:: 

    Resist the urge to build your own time range query using the ``.map()`` query method.  This will result in slow queries.  Instead, use two ``TimeQuery`` instances combined with the ``&`` or ``|`` operator.


Keeping The Index Intact
^^^^^^^^^^^^^^^^^^^^^^^^

TinyFlux must build an index when it is initialized as it currently does not save the index upon closing.  If the workflow for the session is read-only, then the index state will never be modified.  If, however, a TinyFlux session consists of a mix of writes and reads, then the index will become invalid if at any time, a Point is inserted out of time order.

>>> from tinyflux import TinyFlux, Point
>>> from datetime import datetime, timedelta, timezone
>>> db = TinyFlux("my_db.csv")
>>> t = datetime.now(timezone.utc) # current time
>>> db.insert(Point(time=t))
>>> db.index.valid
True
>>> db.insert(Point(time=t - timedelta(hours=1))) # a Point out of time order
>>> db.index.valid
False

If ``auto-index`` is set to ``True`` (the default setting), then the next read will rebuild the index, which may just seem like a very slow query.  For smaller datasets, re-indexing is usually not noticeable.

.. hint::
    
    If possible, Points should be inserted into TinyFlux in time-order.
