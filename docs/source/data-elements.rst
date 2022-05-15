Elements of Data in TinyFlux
----------------------------

Data elements and terms in TinyFlux mostly mirror those of InfluxDB.  The following is a list of TinyFlux terms and concepts.  Click on a term, or read on below.

* :ref:`point`
* :ref:`timestamp`
* :ref:`measurement`
* :ref:`tag set`
* :ref:`tag key`
* :ref:`tag value`
* :ref:`field set`
* :ref:`field key`
* :ref:`field value`


Point
^^^^^

The atomic data unit of TinyFlux.  Consists of a :ref:`measurement`, :ref:`timestamp`, :ref:`tag set`, and a :ref:`field set`.  In the primary disk CSV storage, all attributes are serialized to unicode using the system default encoding.


In Python:

>>> from tinyflux import Point
>>> from datetime import datetime, timezone
>>> p = Point(
...     time=datetime.now(timezone.utc),
...     measurement="thermostat home",
...     tags={
...         "location": "bedroom",
...         "scale": "fahrenheit",
...     },
...     fields={
...         "temp": "70.0",
...     }
... )

On disk:

.. code-block:: bash

   2022-05-13T23:19:46.573233,thermostat home,_tag_location,bedroom,_tag_scale,fahrenheit,_field_temp,70.0


Timestamp
^^^^^^^^^

The time associated with a :ref:`point`.  As an attribute of a :ref:`point`, it is a Python `datetime`_ object.  Regardless of its state, when it is inserted into a TinyFlux database, it will become a timezone aware object cast to the UTC timezone.

On disk, it is serialized as a `ISO 8601`_ formatted string and occupies the first column of the default CSV storage class.

In Python:

>>> Point()

On disk:

.. code-block:: bash

   2022-05-13T23:19:46.573233,_default


For details on time's relationship with TinyFlux, see :doc:`time`.


Measurement
^^^^^^^^^^^

A measurement is a collection of :ref:`Points<point>`, much like a table in a relational database.  It is a string in memory and on disk.  TinyFlux provides a convenient method for interacting with the :ref:`Points<point>` through the ``db.measurement(...)`` method.

In Python:

>>> Point(measurement="cities")

On disk:

.. code-block:: bash

   2022-05-13T23:19:46.573233,cities


See :doc:`measurements` for more details.


Tag Set
^^^^^^^

A tag set (or "tags") is the collection of :ref:`tag keys<tag key>` and :ref:`tag values<tag value>` belonging to a :ref:`point`.  TinyFlux is schemaless, so any Point can contain zero, one, or more tag keys and associated tag values.  Tag keys and tag values are both strings. Tag keys and their values map to Points with a hashmap in a TinyFlux index, providing for efficient retrieval.  In a well-designed TinyFlux database, the number of distinct tag values should not be as numerous as the :ref:`field values<field value>`.  On disk, tag sets occupy side-by-side columns- one for the tag key and one for the tag value.

In Python:

>>> Point(
...     tags={
...         "city": "LA",
...         "neighborhood": "Chinatown",
...         "food": "good",
...     }
... )

On disk:

.. code-block:: bash

   2022-05-13T23:19:46.573233,_default,_tag_city,LA,_tag_neighborhood,Chinatown,_tag_food,good


Tag Key
^^^^^^^

A tag key is the identifier for a :ref:`tag value` in a :ref:`tag set`.  On disk, a tag key is prepended with ``_tag_``.

In the following, the tag key is ``city``.

>>> tags = {"city": "Los Angeles"}


Tag Value
^^^^^^^^^

A tag value is the associated value for a tag key in a :ref:`tag set`.  On disk, it occupies the column next to that of the its tag key.

In the following, the tag value is ``Los Angeles``.

>>> tags = {"city": "Los Angeles"}


Field Set
^^^^^^^^^

A field set (or "fields") is the collection of :ref:`field keys<field key>` and :ref:`field values<field value>` belonging to a :ref:`point`.  TinyFlux is schemaless, so any Point can contain zero, one, or more field keys and associated field values.  Field keys are strings while field values are numeric (in Python, ``float`` or ``int``). Field keys and their values **do not** map to Points in a TinyFlux index as it is assumed that the number of their distinct values is too numerous.  On disk, field sets occupy side-by-side columns- one for the field key and one for the field value.

In Python:

>>> Point(
...     fields={
...         "num_restaurants": 12,
...         "num_boba_shops": 3,
...     }
... )

On disk:

.. code-block:: bash

   2022-05-13T23:19:46.573233,_default,_field_num_restaurants,12,_field_num_boba_shops,3


Field Key
^^^^^^^^^

A field key is the identifier for a :ref:`field value` in a :ref:`field set`.  On disk, a field key is prepended with ``_field_``.

In the following, the field key is ``num_restaurants``.

>>> fields = {"num_restaurants": 12}


Field Value
^^^^^^^^^^^

A field value is the associated value for a :ref:`field key` in a :ref:`field set`.  On disk, it occupies the column next to that of the its field key.

In the following, the field value is ``12``.

>>> fields = {"num_restaurants": 12}


.. _datetime: https://docs.python.org/3/library/datetime.html
.. _ISO 8601: https://en.wikipedia.org/wiki/ISO_8601
 