.. image:: https://github.com/citrusvanilla/tinyflux/blob/master/artwork/tinyfluxdb-light.png?raw=true#gh-dark-mode-only
    :width: 500px
   
.. image:: https://github.com/citrusvanilla/tinyflux/blob/master/artwork/tinyfluxdb-dark.png?raw=true#gh-light-mode-only
    :width: 500px

TinyFlux is the tiny time series database optimized for your happiness :)

TinyFlux is a time series version of `TinyDB <https://tinydb.readthedocs.io/en/latest/index.html>`__ that is also written in Python and has no external dependencies.  It's a great companion for small analytics workflows and apps, as well as at-home IOT data stores.  TinyFlux has 100% coverage, tens of thousands of downloads, and no open issues.

|Build Status| |Coverage| |Version| |Downloads|


Quick Links
***********

- `Example Code Snippets`_
- `Full Example Notebooks and Scripts <https://github.com/citrusvanilla/tinyflux/tree/master/examples>`__
- `Documentation <http://tinyflux.readthedocs.org/>`__
- `Changelog <https://tinyflux.readthedocs.io/en/latest/changelog.html>`__
- `Contributing`_


Installation
************

TinyFlux is hosted at `PyPI <https://pypi.org/project/tinyflux/>`__ and is easily downloadable with ``pip``. TinyFlux has been tested with Python 3.7 - 3.12 and PyPy-3.9 on Linux and Windows platforms.

.. code-block:: bash

    $ pip install tinyflux


Introduction
************

TinyFlux is:

- **time-centric:** Python datetime objects are first-class citizens and queries are optimized for time, above all else.

- **optimized for your happiness:** TinyFlux is designed to be simple and
  fun to use by providing a simple and clean API that can be learned in 5 minutes.

- **tiny:** The current source code has 4,000 lines of code (with about 50%
  documentation) and 4,000 lines of tests.  TinyFlux is about 150kb, unzipped.

- **written in pure Python:** TinyFlux needs neither an external server nor any dependencies.

- **works on Python 3.7+ and PyPy-3.9:** TinyFlux works on all modern versions of Python and PyPy.

- **100% test coverage:** No explanation needed.

To get started, head over to the `TinyFlux docs <https://tinyflux.readthedocs.io/>`__.  Examples can be found in the `examples directory <https://github.com/citrusvanilla/tinyflux/tree/master/examples>`__.  You can also discuss topics related to TinyFlux including general development, extensions, or showcase your TinyFlux-based projects on the `GitHub discussion forum <https://github.com/citrusvanilla/tinyflux/discussions>`__.


Example Code Snippets
*********************

Writing to TinyFlux
===================

.. code-block:: python

    >>> from datetime import datetime, timezone
    >>> from tinyflux import TinyFlux, Point

    >>> db = TinyFlux('/path/to/db.csv')

    >>> p = Point(
    ...     time=datetime(2022, 5, 1, 16, 0, tzinfo=timezone.utc),
    ...     tags={"room": "bedroom"},
    ...     fields={"temp": 72.0}
    ... )
    >>> db.insert(p, compact_key_prefixes=True)


Querying TinyFlux
=================

.. code-block:: python

    >>> from tinyflux import FieldQuery, TagQuery, TimeQuery

    >>> # Search for a tag value.
    >>> Tag = TagQuery()
    >>> db.search(Tag.room == 'bedroom')
    [Point(time=2022-05-01T16:00:00+00:00, measurement=_default, tags=room:bedroom, fields=temp:72.0)]

    >>> # Search for a field value.
    >>> Field = FieldQuery()
    >>> db.select("tag.room", Field.temp > 60.0)
    ["bedroom"]

    >>> # Search for a time value.
    >>> Time = TimeQuery()
    >>> time_start = Time >= datetime(2019, 1, 1, tzinfo=timezone.utc)
    >>> time_end = Time < datetime(2023, 1, 1, tzinfo=timezone.utc)
    >>> db.count(time_start & time_end)
    1


Full Example Notebooks and Workflows
************************************

The `examples <https://github.com/citrusvanilla/tinyflux/tree/master/examples>`__ directory of this repository contains three common uses cases for TinyFlux and the associated boilerplate to get you started:

1. `Loading a TinyFlux DB from a CSV <https://github.com/citrusvanilla/tinyflux/blob/master/examples/1_initializing_and_loading_new_db.ipynb>`__
2. `Local Analytics Workflow with a TinyFlux Database <https://github.com/citrusvanilla/tinyflux/blob/master/examples/2_analytics_workflow.ipynb>`__
3. `TinyFlux as a MQTT Datastore for IoT Devices <https://github.com/citrusvanilla/tinyflux/blob/master/examples/3_iot_datastore_with_mqtt.py>`__
4. `TinyFlux at the Edge (with Backup Strategy) <https://github.com/citrusvanilla/tinyflux/blob/master/examples/4_backing_up_tinyflux_at_the_edge.py>`__

Tips
****

Checkout some tips for working with TinyFlux `here <https://tinyflux.readthedocs.io/en/latest/tips.html>`__.


TinyFlux Across the Internet
****************************

Articles, tutorials, and other instances of TinyFlux:

- `"Introducing TinyFlux: The Tiny Time Series Database for Python-based IoT & Analytics Applications" <https://citrusvanilla.medium.com/introducing-tinyflux-the-tiny-time-series-database-for-python-based-iot-analytics-applications-c3ef3c3bedf>`__: A Medium.com article announcing the release of TinyFlux
- `"Storing Time Series Data in Python Using TinyFluxDB" <http://www.steves-internet-guide.com/storing-time-series-data-python-using-tinyflux-db/>`__: A tutorial from `Steve's Internet Guide <http://www.steves-internet-guide.com/>`__, a portal for learning MQTT and IoT development for Python


Contributing
************

New ideas, new developer tools, improvements, and bugfixes are always welcome.  Follow these guidelines before getting started:

1. Make sure to read `Getting Started <https://tinyflux.readthedocs.io/en/latest/getting-started.html>`__ and the `Contributing Tooling and Conventions <https://tinyflux.readthedocs.io/en/latest/contributing-tooling.html>`__ section of the documentation.
2. Check GitHub for `existing open issues <https://github.com/citrusvanilla/tinyflux/issues>`__, `open a new issue <https://github.com/citrusvanilla/tinyflux/issues/new>`__ or `start a new discussion <https://github.com/citrusvanilla/tinyflux/discussions/new>`__.
3. To get started on a pull request, fork the repository on GitHub, create a new branch, and make updates.
4. Write unit tests, ensure the code is 100% covered, update documentation where necessary, and format and style the code correctly.
5. Send a pull request.

.. |Build Status| image:: https://github.com/citrusvanilla/tinyflux/actions/workflows/build.yml/badge.svg
   :target: https://github.com/citrusvanilla/tinyflux/actions
.. |Coverage| image:: https://codecov.io/gh/citrusvanilla/tinyflux/branch/master/graph/badge.svg?token=IEGQ4E57VA
   :target: https://app.codecov.io/gh/citrusvanilla/tinyflux
.. |Version| image:: http://img.shields.io/pypi/v/tinyflux.svg
   :target: https://pypi.python.org/pypi/tinyflux/
.. |Downloads| image:: https://img.shields.io/pypi/dm/tinyflux.svg
   :target: https://pypi.python.org/pypi/tinyflux/
