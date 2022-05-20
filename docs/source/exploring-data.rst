Exploring Data
==============

An understanding of how queries in TinyFlux work can be applied to several database operations.

Query-based Exploration
-----------------------

The primary method for query usage is through the ``.search(query)``.  Other useful search methods are below:

**.contains(query) <--> Check if the database contains any Points matching a Query**

This returns a simple boolean value and is the fastest search op.

>>> # Check if db contains any Points for Los Angeles after the start of 2022.
>>> from datetime import datetime
>>> from zoneinfo import ZoneInfo
>>> q1 = TagQuery().city == "Los Angeles"
>>> q2 = TimeQuery() >= datetime(2022, 1, 1, tzinfo = ZoneInfo("US/Pacific"))
>>> db.contains(q1 & q2)


**.count(query) <--> Count the number of Points matching a Query**

This returns an integer.

>>> # Count the number of Points for Los Angeles w/ a temp over 100 degrees.
>>> q1 = TagQuery().city == "Los Angeles"
>>> q2 = FieldQuery().temperature_f > 100.0
>>> db.count(q1 & q2)


**.get(query) <--> Get the first Point in the database matching a Query**

This returns a Point instance, or ``None`` if no Points were found.

>>> # Return the first Point in the db for LA w/ more than 1 inch of precipitaion.
>>> q1 = TagQuery().city == "Los Angeles"
>>> q3 = FieldQuery().preciptation > 1.0
>>> db.get(q1 & q3) 


**.search(query) <--> Get all the Points in the database matching a Query**

This is the primary method for querying the database, and  returns a list of Point instances, sorted by timestamp.

>>> # Get all Points in the DB for Los Angeles in 2022 in which the AQI was "hazardous".
>>> from datetime import datetime
>>> from zoneinfo import ZoneInfo
>>> q1 = TagQuery().city == "Los Angeles"
>>> q2 = TimeQuery() >= datetime(2022, 1, 1, tzinfo = ZoneInfo("US/Pacific"))
>>> q3 = TimeQuery() < datetime(2023, 1, 1, tzinfo = ZoneInfo("US/Pacific"))
>>> q4 = FieldQuery().air_quality_index > 100 # hazardous is over 100
>>> db.search(q1 & q2 & q3 & q4)

**.select(attributes, query) <--> Get attributes from Points in the database matching a Query**

This returns a list of attributes from Points matching the Query.  Similar to SQL "select".

>>> # Get the time, city, and air-quality index ("AQI") for all Points with an AQI over 100.
>>> q = FieldQuery().aqi > 100
>>> db.select("fields.aqi", q)
[132]
>>> db.select(("time", "city", "fields.aqi"), q)
[(datetime.datetime(2020, 9, 15, 8, 0, tzinfo=datetime.timezone.utc), "Los Angeles", 132)]


Attribute-based Exploration
---------------------------

The database can also be explored based on attributes, as opposed to queries.


**.get_measurements() <--> Get all the measurements in the database**

This returns an alphabetically-sorted list of measurements in the database.

>>> db.insert(Point(measurement="cities"))
>>> db.insert(Point(measurement="stock prices"))
>>> db.get_measurements()
>>> ["cities", "stock prices"]


**.get_field_keys() <--> Get all the field keys in the database**

This returns an alphabetically-sorted list of field keys in the database.

>>> db.insert(Point(fields={"temp_f": 50.2}))
>>> db.insert(Point(fields={"price": 2107.44}))
>>> db.get_field_keys()
["temp_f", "price"]


**.get_field_values(field_key) <--> Get all the field values in the database**

This returns all the values for a specified field_key, in order of insertion order in the database.  This might be useful for determining a range of values a field could take.

>>> db.insert(Point(fields={"temp_f": 50.2}))
>>> db.insert(Point(fields={"price": 2107.44}))
>>> db.get_field_values("temp_f")
[50.2]


**.get_tag_keys() <--> Get all the tag keys in the database**

This returns an alphabetically-sorted list of tag keys in the database.

>>> db.insert(Point(tags={"city": "LA"}))
>>> db.insert(Point(tags={"company": "Amazon.com, Inc."}))
>>> db.get_tag_keys()
["city", "company"]


**.get_tag_values([tag_key]) <--> Get all the tag values in the database**

This returns all the values for a list of specified tag keys.

>>> db.insert(Point(tags={"city": "LA"}))
>>> db.insert(Point(tags={"company": "Amazon.com, Inc."}))
>>> db.get_tag_values()
{"city": ["Los Angeles"], "company": ["Amazon.com, Inc."]}


**.get_timestamps() <--> Get all the timestamps in the database**

This returns all the timestamps in the database by insertion order.

>>> from datetime import datetime
>>> from zoneinfo import ZoneInfo
>>> time_2022 = datetime(2022, 1, 1, tzinfo = ZoneInfo("US/Pacific"))
>>> time_1900 = datetime(1900, 1, 1, tzinfo = ZoneInfo("US/Pacific"))
>>> db.insert(Point(time=time_2022))
>>> db.insert(Point(time=time_1900))
>>> db.get_timestamps()
[datetime.datetime(2022, 1, 1, 8, 0, tzinfo=datetime.timezone.utc), datetime.datetime(1900, 1, 1, 8, 0, tzinfo=datetime.timezone.utc)]


Full Dataset Exploration
------------------------

Sometimes access to all the data is needed.  There are two methods for doing so- one that brings in all the database items into memory, and one that provides a generator that iterates over items one at a time.

**.all() <--> Get all of the points in the database**

This returns all the points in the database by timestamp order.  To retrieve by insertion order, pass ``sorted=False`` argument.  This will bring all of the data into memory at once.

>>> db.all() # Points returned sorted by timestamp.

or

>>> db.all(sorted=False) # Points returned by insertion order.

**iter(db) <--> Iterate over all the points in the database**

This returns a generator over which point-by-point logic can be applied.  This does not pull everything into memory.

>>> iter(db)
<generator object TinyFlux.__iter__ at 0x103e3d970>
>>> for point in db:
...     print(point)
Point(time=2022-01-01T08:00:00+00:00, measurement=_default)
Point(time=1900-01-01T08:00:00+00:00, measurement=_default)

The list of all the data exploration methods covered above:

+------------------------------------+------------------------------------------------------------------+
| **Query-based Exploration**                                                                           |
+------------------------------------+------------------------------------------------------------------+
| ``db.contains(query)``             | Whether or not the database contains any points matching a query |
+------------------------------------+------------------------------------------------------------------+
| ``db.count(query)``                | Count the number of points matching a query                      |
+------------------------------------+------------------------------------------------------------------+
| ``db.get(query)``                  | Get one point from the database matching a query                 |
+------------------------------------+------------------------------------------------------------------+
| ``db.search(query)``               | Get all points from the database matching a query                |
+------------------------------------+------------------------------------------------------------------+
| ``db.select(attrributes, query)``  | Get attributes froms points matching a query                     |
+------------------------------------+------------------------------------------------------------------+
| **Attribute-based Exploration**                                                                       |
+------------------------------------+------------------------------------------------------------------+
| ``db.get_measurements()``          | Get the names of all measurements in the database                |
+------------------------------------+------------------------------------------------------------------+
| ``db.get_timestmaps()``            | Get all the timestamps from the database, by insertion order     |
+------------------------------------+------------------------------------------------------------------+
| ``db.get_tag_keys()``              | Get all tag keys from the database                               |
+------------------------------------+------------------------------------------------------------------+
| ``db.get_tag_values()``            | Get all tag values from the database                             |
+------------------------------------+------------------------------------------------------------------+
| ``db.get_field_keys()``            | Get all field keys from the database                             |
+------------------------------------+------------------------------------------------------------------+
| ``db.get_field_values()``          | Get all field values from the database                           |
+------------------------------------+------------------------------------------------------------------+
| **Full Dataset Exploration**                                                                          |
+------------------------------------+------------------------------------------------------------------+
| ``db.all()``                       | Get all points in the database                                   |
+------------------------------------+------------------------------------------------------------------+
| ``iter(db)``                       | Return a generator for all points in the database                |
+------------------------------------+------------------------------------------------------------------+
