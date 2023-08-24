TinyFlux Examples
===================

This directory contains various examples of TinyFlux in action.  While TinyFlux does not have any Python dependencies itself, most of the workflows that it is used in are accompanied by other Python libraries.  The other libraries needed to run the examples contained in this directory are found in this directory's `requirements.txt` file.  You may install them by ``cd``-ing into this directory and running:

.. code-block:: bash

    $ pip install requirements.txt


Example 1: Loading a TinyFlux DB from a CSV
-------------------------------------------

``examples/1_initializing_and_loading_new_db.ipynb``

This example demonstrates a common workflow using TinyFlux, which is to create and load a new TinyFlux database with data from an existing CSV.  It demonstrates creating new Point objects with associated timezone-aware datetime objects and inserting them into the database.

To run the example locally, you'll need to install `Jupyter Notebook <https://jupyter.org/>`_ and start a iPython kernel.  It's a simple process, follow along with the link.


Example 2: Local Analytics Workflow with a TinyFlux Database
------------------------------------------------------------

``examples/2_analytics_workflow.ipynb``

This example demonstrates how the TinyFlux database created in the previous example serves as the source-of-truth for a simple exploratory analysis, using the example of California Air Quality Index (AQI) measurements for the years 2019 and 2020.  As this example is a comparative analysis of data across years, TinyFlux and other time-based data stores are a natural candidate for querying and storing the data.

This example uses the beautfiul `Plotly <https://plotly.com/>`_ library for charts and graphics, in addition to Jupyter Notebook.  To install Plotly:

.. code-block:: bash

    $ pip install plotly


Example 3: TinyFlux as a MQTT Datastore for IOT Devices 
-------------------------------------------------------

``examples/3_iot_datastore_with_mqtt.py``

This example demonstrates how TinyFlux can serve as the primary datastore for IOT devices sending data through the `MQTT <https://mqtt.org/>`_ protocol.  The script initializes an MQTT client that subscribes to a sample topic from a test MQTT broker running in the cloud.  The client listens for messages and places them into a queue where a simple worker in a background thread picks up the messages and writes them to TinyFlux.

To run this example locally, you'll need the `Python MQTT client <https://www.eclipse.org/paho/index.php?page=clients/python/index.php>`_ from Eclipse to serve as a bridge.  You may use the same client to publish messages, though the command line `Mosquitto client <https://mosquitto.org/>`_--also from Eclipse--is the preferred method.  To install Paho:

.. code-block:: bash

    $ pip install paho-mqtt
  
To install Mosquitto using ``brew``:

.. code-block:: bash

    $ brew install mosquitto


Example 4: TinyFlux at the Edge (with a backup schedule)
--------------------------------------------------------

``examples/4_backing_up_tinyflux_at_the_edge.py``

This example demonstrates how TinyFlux can be backed up to a remote datastore when using TinyFlux as a datastore at the edge.  The benefit of this method is using a python-based scheduler in the same process as the capture/store control loop.  To run this example you will need to have an influx instance running, along with the `schedule` and `influx-client` pip libraries.

.. code-block:: bash

    $ pip install schedule
    $ pip install 'influxdb-client[ciso]'
  

Have Other Use-Cases for TinyFlux?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We'd love to see them. `Share in a GitHub discussion here <https://github.com/citrusvanilla/tinyflux/discussions>`_.