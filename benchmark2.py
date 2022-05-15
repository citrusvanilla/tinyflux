"""
import os
import cProfile
from tinyflux import FieldQuery, TinyFlux, TagQuery

data_dir = os.path.join(os.getcwd(), "performance_tests", "data.nosync")
f = os.path.join(data_dir, "tinyflux_indexed_master.csv")
db = TinyFlux(f)
db.reindex()
q1 = FieldQuery().AQI > 5000
q2 = TagQuery().City == "Los Angeles-Long Beach-Anaheim"


cProfile.run("r = db.count(q1 & q2)", sort="cumtime")


import csv
import cProfile
from datetime import datetime
import time
import os
import random

from zoneinfo import ZoneInfo

from tinyflux import TinyFlux, Point, TimeQuery, TagQuery, FieldQuery

# Data Directory.
current_dir = os.getcwd()

# Bring in data.
source_file = os.path.join(
    current_dir,
    "performance_tests",
    "data.nosync",
    "daily_aqi_by_cbsa_2020.csv",
)

tmp_file = os.path.join(
    current_dir, "performance_tests", "data.nosync", "daily_aqi_index_test.csv"
)

# Init DB.
db = TinyFlux(tmp_file, auto_index=False)
db.remove_all()

# Bring in original csv.
print("loading original data...")
points = []
tz = ZoneInfo("US/Pacific")

with open(source_file) as f:
    r = csv.reader(f, quotechar='"')
    for i, row in enumerate(r):
        if i == 0:
            continue

        city = row[0].split(",")[0]
        state = row[0].split(", ")[1]
        cbsa_code = str(row[1])
        date = datetime.strptime(row[2], "%Y-%m-%d").replace(tzinfo=tz)
        try:
            aqi = float(row[3])
        except:
            aqi = None

        category = row[4]
        parameter = row[5]
        site = row[6]
        try:
            number_of_sites = int(row[7])
        except:
            number_of_sites = None

        points.append(
            Point(
                time=date,
                tags={
                    "city": str(city),
                    "state": str(state),
                    "cbsa_code": str(cbsa_code),
                    "category": str(category),
                    "parameter": str(parameter),
                    "site": str(site),
                },
                fields={
                    "aqi": aqi,
                    "number_of_sites": number_of_sites,
                },
            )
        )

print("original data loaded.")

print("shuffling list...")
random.shuffle(points)
print("list shuffled.")

print("building tinyflux db.")
db.insert_multiple(points)
print("points inserted.")

print("building an index.")
start = time.time()
db.reindex()
end = time.time()
print(f"building index took {round(end-start, 4)} seconds.")

print("searching...")

tz = ZoneInfo("US/Pacific")
jan_start = datetime(2020, 1, 1).replace(tzinfo=tz)
feb_start = datetime(2020, 2, 1).replace(tzinfo=tz)


q1 = TimeQuery() >= jan_start
q2 = TimeQuery() < feb_start
q3 = TagQuery().city == "Los Angeles-Long Beach-Anaheim"
q4 = FieldQuery().aqi >= 100

cProfile.run("r = db.search(q1 & q2 & q3 & q4)", sort="cumtime")

print(db._index.search(q1 & q2 & q3 & q4).items)

for p in r:
    print(p)
"""

"""
import csv
import cProfile
from datetime import datetime
import time
import os
import random

from zoneinfo import ZoneInfo

from tinyflux import TinyFlux, Point

# Data Directory.
current_dir = os.getcwd()

# Bring in data.
source_file = os.path.join(
    current_dir,
    "performance_tests",
    "data.nosync",
    "daily_aqi_by_cbsa_2020.csv",
)

tmp_file = os.path.join(
    current_dir,
    "performance_tests",
    "data.nosync",
    "daily_aqi_index_insert_test.csv",
)

# Init DB.
db = TinyFlux(tmp_file, auto_index=True)
db.remove_all()

# Bring in original csv.
print("loading original data...")
points = []
tz = ZoneInfo("US/Pacific")

test_len = 10000

with open(source_file) as f:
    r = csv.reader(f, quotechar='"')
    for i, row in enumerate(r):
        if i == 0:
            continue

        city = row[0].split(",")[0]
        state = row[0].split(", ")[1]
        cbsa_code = str(row[1])
        date = datetime.strptime(row[2], "%Y-%m-%d").replace(tzinfo=tz)
        try:
            aqi = float(row[3])
        except:
            aqi = None

        category = row[4]
        parameter = row[5]
        site = row[6]
        try:
            number_of_sites = int(row[7])
        except:
            number_of_sites = None

        points.append(
            Point(
                time=date,
                tags={
                    "city": str(city),
                    "state": str(state),
                    "cbsa_code": str(cbsa_code),
                    "category": str(category),
                    "parameter": str(parameter),
                    "site": str(site),
                },
                fields={
                    "aqi": aqi,
                    "number_of_sites": number_of_sites,
                },
            )
        )
        if i == test_len:
            break


def f():
    """ """
    for i in points:
        db.insert(i)


cProfile.run("f()", sort="cumtime")
"""

from datetime import datetime
import os
from zoneinfo import ZoneInfo
import cProfile

from tinydb import TinyDB, Query

current_dir = os.path.dirname(os.path.realpath(__file__))

data_dir = os.path.join(current_dir, "performance_tests", "data.nosync")

# Master TinyDB file.
td_file_medium = os.path.join(data_dir, "tinydb_medium.json")

db = TinyDB(td_file_medium)


tz = ZoneInfo("US/Pacific")
june = datetime(2020, 6, 1).replace(tzinfo=tz)
july = datetime(2020, 7, 1).replace(tzinfo=tz)

q1 = Query().time.map(lambda x: datetime.fromisoformat(x)) >= june
q2 = Query().time.map(lambda x: datetime.fromisoformat(x)) < july
q3 = Query().city == "Los Angeles-Long Beach-Anaheim"
q4 = Query().aqi > 100

cProfile.run("r = db.search(q4)", sort="cumtime")
