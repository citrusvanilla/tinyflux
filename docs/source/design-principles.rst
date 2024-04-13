TinyFlux Design Principles
==========================

InfluxDB implements optimal design principles for time series data. Some of these design principles have associated tradeoffs in performance. Design principles are discussed below.

- :ref:`Prioritize High-Speed Writes`
- :ref:`Minimize Memory Footprint`
- :ref:`Prioritize Searches for Time`
- :ref:`Schemaless design`
- :ref:`IDs and Duplicates`


Prioritize High-Speed Writes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Time series data is often write-heavy, and in cases when a time series database is used as a real-time data store, the frequency of writes can be quite high.  TinyFlux has been designed to minimize any disruptions to writing to disk in a single thread in as fast a manner as possible.  To accomplish this, TinyFlux utilizes a default CSV store which supports nearly instantaneous appends, regardless of underlying file size.  TinyFlux will also invalidate its index if upon any insert, the timestamp for a Point precedes that of the most-recent insert.  TinyFlux will not attempt to rebuild its index upon invalidation during a write op.


Minimize Memory Footprint
^^^^^^^^^^^^^^^^^^^^^^^^^
While it would be great if databases could live in memory, this is not a reasonable design choice for everyday users.  TinyFlux has been designed to never read the entire contents of its storage into memory unless explicitly asked to do so, and to balance the 
need for fast querying with a small memory footprint, TinyFlux builds an internal index.  This index is generally about 80% smaller than the memory required to hold the entire dataset in memory, and still allows for query performance to equal or surpass that of keeping the database in memory.  For removals and updates, TinyFlux still visits all items in storage, but evaluates each item one at a time and writes to temporary storage before finally replacing the original storage with the updated one.  TinyFlux also does not rewrite data in time-ascending order, as is the case with InfluxDB, as this would require either the entire dataset to be read into memory, or a computationally expensive eternal merge sort to be executed on disk.


Prioritize Searches for Time
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
TinyFlux builds an index on time by keeping a sorted container data structure of timestamps in memory, and searches over the index quickly by parsing queries and invoking optimized search algorithms for sorted containers to retrieve candidate Points quickly.  This reduces potentially slow and exhaustive evaluations significantly.


Schemaless design
^^^^^^^^^^^^^^^^^
Even though row-based data stores like CSV are not thought of as "schemaless", TinyFlux nonetheless allows for datasets to have flexible schemas so that signals that change over time, or multiple signals from multiple sources, can all occupy space in the same datastore.  This allows the user to focus less on database design and more on capturing and analyzing data.


IDs and Duplicates
^^^^^^^^^^^^^^^^^^
TinyFlux does not keep IDs as it is assumed data points are unique by their combination of timestamp and tag set.  To this end, TinyFlux also does not currently have a mechanism for checking for duplicates. Searches matching duplicate Points will return duplicates.