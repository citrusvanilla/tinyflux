r"""An example of using TinyFlux as an IOT datastore for MQTT messages.

To test this script, you must be able to publish to the test Mosquitto MQTT
broker, which is a free broker running at test.mosquitto.org.

Port 1883 is unencrypted and unauthenticated, so it should only be used for
test purposes.

This script listens for messages being published to one topic on this broker.

To download a Linux MQTT command line client to publish messages, use brew:

    $ brew install mosquitto

In one terminal window/process, start this script:

    $ python 3_iot_datastore_with_mqtt.py

You should see "Connecting to test.mosquitto.org... success.".

In a second terminal window/process, copy and paste the following, which
publishes a sample JSON encoded message to the test MQTT broker:

    $ mosquitto_pub \
        -h test.mosquitto.org \
        -t tinyflux_test_topic \
        -m "{\"device\":\"thermostat\",\"temperature\":70.0,\"humidity\":0.25}"

This multi-threaded approach to logging MQTT messages comes from Steve Cope's
"Logging MQTT Sensor Data to SQLite DataBase With Python", available at
http://www.steves-internet-guide.com.

Author:
    Justin Fung (justincaseyfung@gmail.com)
"""
from datetime import datetime, timezone
from queue import Queue
import json
import threading

import paho.mqtt.client as mqtt

from tinyflux import TinyFlux, Point


MQTT_HOST = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
MQTT_TOPIC = "tinyflux_test_topic"

TINYFLUX_DB = "my_tinyflux_mqtt_database.db"

# TinyFlux DB.
db = TinyFlux(TINYFLUX_DB)

# Interthread queue.
q = Queue()

# Init but do not set a threading exit event for graceful exit.
exit_event = threading.Event()


def on_connect(client, *args):
    """Define the on_connect callback.

    Subscribes to the default topic after connection has been made.

    Args:
        client: A Paho MQTT client instance.
    """
    # Log.
    print("Connection success.\n")

    # Subscribe to the topic of interest.
    client.subscribe(MQTT_TOPIC)

    # Log.
    print(f"Subscribed to '{MQTT_TOPIC}' and waiting for messages.\n")

    return


def on_disconnect(_, __, rc):
    """Define the on_disconnect callback.

    See http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/
        mqtt-v3.1.1-os.html#_Toc398718035
    for return codes descriptions.

    Args:
        rc: the disconnection result
    """
    # Log.
    if rc == 0:
        print("Disconnected .\n")
    else:
        print(f"Unexpected disconnection, return code {rc}.\n")

    return


def on_message(_, __, msg):
    """Define callback for new message event.

    Unmarshalls the message and writes new data to the interthread queue.

    Args:
        msg: A Paho MQTT message object.
    """
    # Unmarshall the message.
    topic = msg.topic
    payload = json.loads(msg.payload.decode("utf-8"))

    # Log.
    print(f'• Message received for topic "{topic}"... ', flush=True, end="")

    # Put the message data on the queue.
    q.put({"topic": topic, "payload": payload})

    return


def initialize_mqtt_client():
    """Initialize and return the MQTT client.

    Returns:
        A Paho MQTT Client object.
    """
    # Initialize the client.
    client = mqtt.Client()

    # Register callbacks.
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    return client


def run_tinyflux_worker():
    """Define the TinyFlux worker thread.

    Loops until the exit event is set.  Pops from the interthread queue
    and writes to TinyFlux.
    """
    # Loop until exit_event is set by main thread.
    while True:
        # Check the queue for new packets.
        if not q.empty():
            # Unpack MQTT packet.
            data = q.get()
            topic = data["topic"]
            payload = data["payload"]

            try:
                device = payload["device"]
                temperature = payload["temperature"]
                humidity = payload["humidity"]

                # Initialize a TinyFlux Point.
                p = Point(
                    time=datetime.now(timezone.utc),
                    measurement=topic,
                    tags={"device": device},
                    fields={"temperature": temperature, "humidity": humidity},
                )

                # Insert the Point into the DB.
                db.insert(p)

                print(f'write to "{TINYFLUX_DB}" successful!')
            except Exception as e:
                print(f"\n  **Problem attempting to write: {e}")

        # Check for exit condition.
        if exit_event.is_set():
            break

    return


def main():
    """Define main."""
    # Log.
    print(f"Connecting to {MQTT_HOST}... ", flush=True, end="")

    # Initialize TinyFlux worker thread.
    t = threading.Thread(target=run_tinyflux_worker)

    # Start the worker thread.
    t.start()

    # Initialise MQTT CLIENT.
    client = initialize_mqtt_client()

    # Start MQTT network loop in a threaded interface to unblock main thread.
    client.loop_start()

    # Connect to the broker.
    client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)

    # Keep this process running until SIGINT received.
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\nExiting gracefully... ", flush=True, end="")

    # SIGINT received: set the exit event so the worker thread knows to exit.
    exit_event.set()

    # Await spawned thread.
    t.join()

    # Stop network loop.
    client.loop_stop()

    # Close db.
    db.close()

    # Log.
    print("done.")


if __name__ == "__main__":
    main()
