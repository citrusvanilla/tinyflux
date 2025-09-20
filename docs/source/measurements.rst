Working with Measurements
-------------------------

TinyFlux supports working with multiple measurements. A measurement is analogous to a "table" in traditional RDMS.  By accessing TinyFlux through a measurement, the same database API is utilized, but with a filter for the passed measurement.

To access TinyFlux through a measurement, use ``db.measurement(name)``:

>>> db = TinyFlux("my_db.csv")
>>> m = db.measurement("my_measurement")
>>> m.insert(Point(time=datetime(2022, 1, 1, tzinfo=timezone.utc), tags={"my_tag_key": "my_tag_value"}))
>>> m.all()
[Point(time=2022-01-01T00:00:00+00:00, measurement=my_measurement, tags=my_tag_key:my_tag_value)]

Measurements support all the same methods as the main database, including ``insert_multiple()`` with batch processing:

>>> # Insert multiple points into a specific measurement
>>> points = [Point(...), Point(...), ...]
>>> m.insert_multiple(points, batch_size=1000)  # Configurable batching
>>> for point in m:
>>>     print(point)
Point(time=2022-01-01T00:00:00+00:00, measurement=my_measurement, tags=my_tag_key:my_tag_value)

.. note:: 

    TinyFlux uses a measurement named ``_default`` as the default measurement.

To remove a measurement and all its points from a database, use:

>>> db.drop_measurement('my_measurement')

or

>>> m.remove_all()

To get a list with the names of all measurements in the database:

>>> db.get_measurements()
["my_measurement"]
