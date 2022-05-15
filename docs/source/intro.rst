Introduction
============

TinyFlux combines the simplicity of the document-oriented TinyDB_ with the concepts and design of the fully-fledged time series database known as InfluxDB_.

TinyFlux is a pure Python module that supports database-like operations on an in-memory or file datastore.  It is optimized for time series data and as such, is considered a "time series database" (or "tsdb" for short).  It is not, however, a database server that supports traditional RDMS features like the management of concurrent connections, management of indexes in background processes, or the provisioning of access control.  Before using TinyFlux, you should be sure that TinyFlux is right for your intended use-case.


Why Should I Use TinyFlux?
--------------------------

**In TinyFlux, time comes first.**

- Time in TinyFlux is a first-class citizen. TinyFlux expects and handles Python datetime objects with ease.  Queries are optimized for time, above all else.

**TinyFlux is a real time series database.**

- Concepts around TinyFlux are based on InfluxDB.  If you are looking for a gradual introduction into the world of time series databases, this is a great starting point.  If your workflow outgrows the offerings of TinyFlux, you can jump to InfluxDB with very little introduction needed.

**TinyFlux is written in pure, standard library Python.**

- TinyFlux needs neither an external server nor any dependencies and works on all modern versions of Python.

**TinyFlux is optimized for your happiness.**

- Like TinyDB_, TinyFlux is designed to be simple and easy to use by providing a straight-forward and clean API.

**TinyFlux is tiny.**

- The current source code has 2000 lines of code (with about 50% documentation) and 2000 lines of tests.

**TinyFlux has 100% test coverage.**

- No explanation needed.


If you have a moderate amount of time series data without the need or desire to provision and manage a full-fledged server and its configuration, and you want to interface easily with the greater Python ecosystem, TinyFlux might be the right choice for you.


When To Look at Other Options
-----------------------------

You should not use TinyFlux if you need advanced database features like:

- access from multiple processes or threads
- an HTTP server
- management of relationships between tables
- access-control and users
- `ACID guarantees <https://en.wikipedia.org/wiki/ACID>`_
- High performance as the size of your dataset grows

If you have a large amount of data, or you need advanced features and high performance, consider using databases like SQLite_ or InfluxDB_.


What's the difference between TinyFlux and TinyDB?
--------------------------------------------------

At its core, TinyFlux is a *time series database* while TinyDB is a *document-oriented database*.

Let's break this down:

**In TinyFlux, time is a "first-class citizen".**.

- In TinyDB, there is no special handling of time.

**A TinyFlux database expects Python datetime objects to be passed with each and every data point.**

- TinyDB does not accept datetime objects directly. In TinyDB, attributes representing time must be serialized and deserialized by the user, or an extension must added onto TinyDB to handle datetime objects.

**In TinyFlux, queries are optimized for time.**

- TinyFlux builds a small index in memory which includes an index on timestamps. This provides for ultra-fast search and retrieval of data when queries are time-based. TinyDB has no special mechanism for querying attributes of different types.

**Data in TinyFlux is written to disk in "append-only" fashion.**

- Irrespective of the current size of the database, inserting is always a constant-time operation on the order of nanoseconds.  TinyFlux is optimized for time series datasets which are often write-heavy, as opposed to document-stores which are traditionally read-heavy. This allows high-frequency signals to be easily handled by TinyFlux. TinyDB does not expect high-frequency writes, and since it reads all data into memory before adding new data, its insert time increases linearly with the size of the database.

**TinyFlux and TinyDB are both "schemaless".**

- This means that attributes and their existence between items may differ with no exceptions being raised.  TinyDB, as a document store, supports the storage of complex types including containers like arrays/lists and objects/dictionaries.  TinyFlux, however, provides for just three types of attributes- numeric, string, and of course, datetime.


Got it, so should I use TinyFlux or TinyDB?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- You should use **TinyFlux** if:
    - Your data is naturally time series in nature. That is, you have many observations of some phenomenon over time with varying measurements. Examples include stock prices, daily temperatures, or the accelerometer readings on a running watch.
    - You will be writing to the database at a regular, high frequency.

- You should use **TinyDB** if:
    - Your data has no time dimension. Examples include a database acting as a phonebook for Chicago, the catalogue of Beatles music, or configuration values for your dashboard app.
    - You will be writing to the database infrequently.


.. References
.. _InfluxDB: https://influxdata.com/
.. _SQLite: https://www.sqlite.org/
.. _TinyDB: https://github.com/msiemens/tinydb