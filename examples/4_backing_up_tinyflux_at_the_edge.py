"""Example of using TinyFlux at the edge along with a backup job.

This is a simple way to capture sensor data and backup in the same
process.

This backs up to influx, but the same principle applies to any
datastore of your choosing.

Requires TinyFlux, influxdb-client, and schedule, downloadable from pip.

See https://github.com/dbader/schedule for documentation.
"""
from datetime import datetime
import time

from influxdb_client import InfluxDBClient, Point as InfluxPoint
from influxdb_client.client.write_api import SYNCHRONOUS

import schedule

from tinyflux import TinyFlux, Point as TinyFluxPoint, TimeQuery

# This is an arbitrary module for a device.
from myThermometer import ThermSensor1  # type: ignore

# Your device.
sensor = ThermSensor1()

# TinyFlux db.
db = TinyFlux("my_db.tinyflux")

# Remote influx instance and associated write API.
client = InfluxDBClient(
    url="http://localhost:8086",
    token="my-token",
    org="my-org",
)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Keep track of the last backup (initialize to Jan. 1, 1 AD).
LAST_BACKUP_TIME = datetime(1, 1, 1)


# The backup job.
def backup_db():
    """Backup TinyFlux db to remote Influx database."""
    global LAST_BACKUP_TIME

    # Get all points since last backup.
    points_needing_backup = db.search(TimeQuery() > LAST_BACKUP_TIME)

    # Turn TinyFlux records into Influx records.
    influx_records = []

    for i in points_needing_backup:
        p = InfluxPoint("my_measurement")

        for tag_key, tag_val in i.tags.items():
            p = p.tag(tag_key, tag_val)

        for field_key, field_val in i.fields.items():
            p = p.field(field_key, field_val)

        influx_records.append(p)

    # Write to Influx.
    try:
        write_api.write(bucket="my-bucket", record=influx_records)

        # Update backup time.
        LAST_BACKUP_TIME = datetime.now()
    except Exception as e:
        print(f"Error backing up to Influx: {e}")

    return


# The frequency with which you execute the backup job.
schedule.every(4).hours.do(backup_db)


def main():
    """Define Main."""
    # Your control loop.
    while True:
        # Get the temperature from your device.
        temperature = sensor.get_temperature()

        # Make a Point and insert the reading into TinyFlux.
        p = TinyFluxPoint(
            measurement="my_measurement",
            tags={"room": "bedroom"},
            fields={"temperature": temperature},
        )

        try:
            db.insert(p)
        except Exception as e:
            print(f"Error writing to DB: {e}")

        # Run the job that is scheduled above (if due).
        schedule.run_pending()

        # Sleep for your predefined sampling interval.
        time.sleep(1)


if __name__ == "__main__":
    main()
