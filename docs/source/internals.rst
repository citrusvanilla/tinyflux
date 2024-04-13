TinyFlux Internals
==================

Storage
-------

TinyFlux ships with two types of storage:

1. A CSV store with is persistent to disk, and 
2. A memory store which lasts only as long as the process in which it was declared.

To use the CSV store, pass a filepath during TinyFlux initialization.

>>> my_database = "db.csv"
>>> db = TinyFlux(my_database)

To use the memory store:

>>> from tinyflux.storages import MemoryStorage
>>> db = TinyFlux(storage=MemoryStorage)

In nearly all cases, users should opt for the former as it persists the data on disk.

The CSV format is familiar to most, but at its heart it's just a row-based datastore that supports sequential iteration and append-only writes.  Contrast this with JSON, which--while fast once loaded into memory--must be loaded entirely into memory and does not support appending.

The usage of CSV offers TinyFlux two distinct advantages for typical time-series workflows:

1. Appends do not require reading of data, and occur in a constant amount of time regardless of the size of the underlying database.
2. Sequential iteration allows for a full read of the data without having to simulateously keep the entirety of the data store in memory all at once.  Logic can be performed on an individual row, and results kept or discarded as desired.

TinyFlux storage is also designed to be extensible.

In case direct access to the storage instance is desired, use the ``storage`` property of the TinyFlux instance.

>>> from tinyflux.storages import MemoryStorage
>>> db = TinyFlux(storage=MemoryStorage)
>>> my_data = db.storage.read()

For more disucssion on storage, see :doc:`design-principles`.


Indexing in TinyFlux
--------------------

By default, TinyFlux will build an internal index when the database is initialized, and again at any point when a read operation is performed after the index becomes invalid.  As TinyFlux's primary storage format is a CSV that is read from disk sequentially, the index allows for efficient retrieval operations that greatly reduce function calls, query evaluations, and the need to deserialize and reserialize data.

.. note:: 

    An index becomes invalid when points are inserted out-of-time-order.  When the ``auto-index`` parameter of ``TinyFlux`` is set to ``True``, the next read operation will rebuild the index.

Building an index is a non-trivial routine that occurs in the same process that TinyFlux is running in.  For smaller amounts of data in a typical analytics workflow, building an index may not even be noticeable.  As the database grows, the time needed to build or rebuild the index grows linearly.  Automatically rebuilding of the index can be turned off by setting ``auto_index`` to ``False`` in the TinyFlux constructor:

>>> db = TinyFlux("my_database.csv", auto_index=False)

Setting this value to ``False`` will remove any indexing-building, but queries will slow down considerably.

A reindex can be manually triggered should the need arise:

>>> db.reindex()

.. warning:: 
    There is usually only one reason to turn off auto-indexing and that is when you are initializing the database instance and need to **immediately** start inserting points, as might be the case in IOT data-capture applications. In all other cases, particularly when reads will make up the majority of your workflow, you should leave ``auto-index`` set to ``True``.

At some level of data, the building of the index will noticeably slow down a workflow.  For tips on how to address growing data, see :doc:`tips`.