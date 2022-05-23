"""An example of using TinyFlux as an IOT datastore.

Based off of "Logging MQTT Sensor Data to SQLite DataBase With Python", at
steves-internet-guide.com/logging-mqtt-sensor-data-to-sql-database-with-python/
by Steve Cope (steve@steves-internet-guide.com).
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

TINYFLUX_DB = "test.csv"

# TinyFlux DB.
db = TinyFlux(TINYFLUX_DB)

# Interthread queue.
q = Queue()

# Exit threading event for graceful exit.
exit_event = threading.Event()


def on_message(_, __, msg):
    """"""
    # Unmarshall the message.
    topic = msg.topic
    payload = json.loads(msg.payload.decode("utf-8"))

    print(f'• Message received for topic "{topic}"... ', flush=True, end="")

    # Put the message data on the queue.
    q.put({"topic": msg.topic, "payload": payload})

    return


def run_tinyflux_worker():
    """runs in own thread to log data"""
    while True:

        # Check the queue for new packets.
        if not q.empty():

            data = q.get()

            if data is None:
                continue

            try:
                # Unpack MQTT packet.
                topic = data["topic"]
                payload = data["payload"]

                device = payload["device"]
                value = payload["value"]

                # Initialize a TinyFlux Point.
                p = Point(
                    time=datetime.now(timezone.utc),
                    measurement=topic,
                    tags={"device": device},
                    fields={"value": value},
                )

                # Insert the Point into the DB.
                db.insert(p)

                print("write to TinyFlux successful!")
            except Exception as e:
                print(f"\n  **Problem with writing: {e}")

        # Check for exit condition.
        if exit_event.is_set():
            break

    return


def on_connect(client, *args):
    """"""
    print("success.\n")

    client.subscribe(MQTT_TOPIC)

    print(f"Subscribed to '{MQTT_TOPIC}' and waiting for messages.\n")

    return


def initialize_mqtt_client(host, port, keep_alive):
    """"""
    # Initialize the client.
    client = mqtt.Client(host, port, keep_alive)

    # Register callbacks.
    client.on_connect = on_connect
    client.on_message = on_message

    return client


def main():
    """ """
    print(f"Connecting to {MQTT_HOST}... ", flush=True, end="")

    # Initialize TinyFlux worker thread.
    t = threading.Thread(target=run_tinyflux_worker)

    # Start the worker thread.
    t.start()

    # Initialise MQTT CLIENT.
    client = initialize_mqtt_client(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)

    # Start MQTT network loop in a threaded interface to unblock main thread.
    client.loop_start()

    # Connect to the broker.
    client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)

    # Keep this process running until SIGINT received.
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Exiting gracefully...")

    # SIGINT received: set the exit event so the worker thread knows to exit.
    exit_event.set()

    # Await spawned thread.
    t.join()

    # Stop network loop.
    client.loop_stop()

    # Close db.
    db.close()


if __name__ == "__main__":
    main()
