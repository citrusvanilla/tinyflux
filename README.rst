.. image:: https://github.com/citrusvanilla/tinyflux/blob/master/artwork/logo-light.png?raw=true#gh-dark-mode-only
    :width: 500
   
.. image:: https://github.com/citrusvanilla/tinyflux/blob/master/artwork/logo-dark.png?raw=true#gh-light-mode-only
    :width: 500

|Build Status| |Coverage| |Version|

Quick Links
***********

- `Example Code`_
- `Documentation <http://tinyflux.readthedocs.org/>`_
- `Changelog <https://tinyflux.readthedocs.io/en/latest/changelog.html>`_
- `Contributing`_

Introduction
************

TinyFlux is a lightweight time series database optimized for your happiness :)  It's a time-centic version of TinyDB, written in pure Python, and has no external dependencies. It's target is small analytics workflows and at-home IOT data stores.

TinyFlux is:

- **time-centric:** Python datetime objects are first-class citizens and time queries are optimized, above all else.

- **optimized for your happiness:** TinyFlux is designed to be simple and
  fun to use by providing a simple and clean API that can be learned in 5 minutes.

- **tiny:** The current source code has 4,000 lines of code (with about 50%
  documentation) and 4,000 lines tests.  TinyFlux is about 150kb, unzipped.

- **written in pure Python:** TinyFlux needs neither an external server nor any dependencies.

- **works on Python 3.7+ and PyPy-3.9:** TinyFlux works on all modern versions of Python
  and PyPy.

- **100% test coverage:** No explanation needed.

To dive straight into all the details, head over to the `TinyFlux docs <https://tinyflux.readthedocs.io/>`_. You can also discuss everything related to TinyFlux like general development, extensions or showcase your TinyFlux-based projects on the ` GitHub discussion forum <https://github.com/citrusvanilla/tinyflux/discussions>.`_.

Supported Python Versions
*************************

TinyFlux has been tested with Python 3.7 - 3.10 and PyPy-3.9.

Example Code
************

Writing to TinyFlux
===================

.. code-block:: python

    >>> from datetime import datetime, timezone
    >>> from tinyflux import TinyFlux, Point

    >>> db = TinyFlux('/path/to/db.csv')

    >>> p = Point(
    ...     time=datetime(2019, 5, 1, 16, 0, tzinfo = timezone.utc),
    ...     tags={"room": "bedroom", "scale": "fahrenheit"},
    ...     fields={"temp": 72.0}
    ... )
    >>> db.insert(p)


Querying TinyFlux
=================

.. code-block:: python

    >>> from tinyflux import FieldQuery, TagQuery, TimeQuery

    >>> # Search for a tag value.
    >>> Room = TagQuery().room
    >>> db.search(Room == 'bedroom')
    [Point(time=2019-05-01T16:00:00+00:00, measurement=_default, tags=room:bedroom; scale:fahrenheit, fields=temp:72.0)]

    >>> # Search for a field value
    >>> Temp = FieldQuery().temp
    >>> db.search(Temp > 60.0)
    [Point(time=2019-05-01T16:00:00+00:00, measurement=_default, tags=room:bedroom; scale:fahrenheit, fields=temp:72.0)]

    >>> # Search for a time value.
    >>> # This demonstrates combining two queries with logical-AND.
    >>> Time = TimeQuery()
    >>> time_start = Time >= datetime(2019, 1, 1, tzinfo = timezone.utc)
    >>> time_end = Time < datetime(2020, 1, 1, tzinfo = timezone.utc)
    >>> db.count(time_start & time_end)
    1

    >>> # Combine two queries with logical-OR.
    >>> Bedroom = TagQuery().room == "bedroom"
    >>> Kitchen = TagQuery().room == "kitchen"
    >>> db.select("tags.room", Bedroom | Kitchen)
    [("bedroom",)]

Measurements
============

Measurements are like tables from relational databases:

.. code-block:: python

    >>> stock_prices = db.measurement('stock prices')
    >>> p = Point(
    ...     time=datetime.now(timezone.utc),
    ...     tags={"company": "Amazon.com, Inc.", "symbol": "AMZN"},
    ...     fields={"price": 2142.25}
    ... )
    >>> stock_prices.insert(p)
    >>> len(stock_prices)
    1


Contributing
************

New ideas, improvements, bugfixes, and new developer tools are always welcome.  Follow these guidelines before getting started:

1. Make sure to read [Getting Started](https://tinyflux.readthedocs.io/en/latest/getting-started.html) and the ["Contributing"](https://tinyflux.readthedocs.io/en/latest/contributing-philosophy.html) section of the documentation.
2. Check GitHub for [existing open issues](https://github.com/citrusvanilla/tinyflux/issues), or [open a new issue](https://github.com/citrusvanilla/tinyflux/issues/new) or [start a new discussion](https://github.com/citrusvanilla/tinyflux/discussions/new).
3. To get started on a pull request, fork the repository on GitHub, create a new branch, and make updates.
4. Write unit tests, ensure the code is 100% covered, update documentation where necessary, and format and style the code correctly.
5. Send a pull request.

.. |Build Status| image:: https://github.com/citrusvanilla/tinyflux/actions/workflows/build.yml/badge.svg
.. |Coverage| image:: https://codecov.io/gh/citrusvanilla/tinyflux/branch/master/graph/badge.svg?token=IEGQ4E57VA
   :target: https://app.codecov.io/gh/citrusvanilla
.. |Version| image:: http://img.shields.io/pypi/v/tinyflux.svg?style=flat-square
   :target: https://pypi.python.org/pypi/tinyflux/
