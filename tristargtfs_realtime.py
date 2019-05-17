from google.transit import gtfs_realtime_pb2 as gtfs_rt
from datetime import timedelta, datetime, time
from tempfile import TemporaryFile
import email.utils
import argparse
import requests
import zipfile
import time
import csv
import os
import io
import re

__title__ = "TristarGTFS-Realtime"
__author__ = "Mikołaj Kuranowski"
__email__ = "mikolaj@mkuran.pl"
__license__ = "MIT"

def is_url(url):
    if url.startswith("https://") or url.startswith("ftp://") or url.startswith("http://"):
        return True
    else:
        return False

def readable_time(departure_time):
    h, m, s = map(int, departure_time.split(":"))
    h = h % 24
    return "{:0>2d}:{:0>2d}".format(h, m)

def no_html(text):
    "Clean text from html tags"
    if text == None or text == "": return ""
    text = text.replace("<br />", "\n").replace("<br>", "\n").replace("<br >", "\n")
    text = text.replace("<p>", "\n\n")
    text = re.sub("<.*?>", "", text)

    return text

class GdanskData:
    def __init__(self, source, debug):
        self.source = source

        self.time = datetime.min

        self.services = set()

        self.debug = bool(debug)

        self.trips_route = {}
        self.stop_trips = {}

        self.gtfs = None
        self.arch = None

    def get_gtfs(self):
        if is_url(self.source):
            print("\033[1A\033[K" + "Requesting GTFS from " + self.source)
            gtfs_request = requests.get(self.source)
            self.time = email.utils.parsedate_to_datetime(gtfs_request.headers["Last-Modified"])
            self.gtfs = TemporaryFile()
            self.gtfs.write(gtfs_request.content)
            self.gtfs.seek(0)

        else:
            print("\033[1A\033[K" + "Reading local GTFS from " + self.source)
            self.gtfs = open(self.source, mode="rb")
            self.time = datetime.fromtimestamp(os.stat(self.source).st_mtime)

        self.arch = zipfile.ZipFile(self.gtfs, mode="r")

    def load_gtfs(self):
        today = datetime.today()

        # If it's before 4 AM use previous services for night routes
        if today.hour < 4:
            today -= timedelta(days=1)

        today_str = today.strftime("%Y%m%d")

        ### ACTIVE DATES ###
        print("\033[1A\033[K" + "GTFS: Loading active services (calendar_dates.txt)")

        with self.arch.open("calendar_dates.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                if row["date"] == today_str: self.services.add(row["service_id"])

        ### TRIP_ID → ROUTE_ID ###
        print("\033[1A\033[K" + "GTFS: Loading active trips (trips.txt)")

        with self.arch.open("trips.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                # Only active trips
                if row["service_id"] not in self.services: continue

                if ":" in row["route_id"]:
                    row["agency_id"], row["route_id"] = row["route_id"].split(":")

                self.trips_route[row["trip_id"]] = row["route_id"]

        ### STOP DEPARTURES ###
        print("\033[1A\033[K" + "GTFS: Loading departures per stop (stop_times.txt)")

        with self.arch.open("stop_times.txt", mode="r") as buffer:
            for row in csv.DictReader(io.TextIOWrapper(buffer, encoding="utf8", newline="")):
                # Only trips which are in self.trips_route are able to havbe RT data
                if row["trip_id"] not in self.trips_route: continue

                stop = row["stop_id"]
                route = self.trips_route[row["trip_id"]]
                static_time = readable_time(row["departure_time"])

                if stop not in self.stop_trips:
                    self.stop_trips[stop] = {}

                if route not in self.stop_trips[stop]:
                    self.stop_trips[stop][route] = {}

                if self.debug and static_time in self.stop_trips[stop][route]:
                    print("\033[1m" + "Duplicate departure for S {} R {} T {}: trip {} and {}".format(
                        stop, route, static_time, self.stop_trips[stop][route][static_time], row["trip_id"],
                    ) + "\033[0m", end="\n\n")

                self.stop_trips[stop][route][static_time] = row["trip_id"]

    def new_gtfs_available(self):
        if is_url(self.source):
            print("\033[1A\033[K" + "Checking if new GTFS is available at " + self.source)
            gtfs_request = requests.get(self.source)
            time = email.utils.parsedate_to_datetime(gtfs_request.headers["Last-Modified"])

        else:
            print("\033[1A\033[K" + "Checking if file at " + self.source + " has changed")
            time = datetime.fromtimestamp(os.stat(self.source).st_mtime)

        if time > self.time: return True
        else: return False

        self.arch = zipfile.ZipFile(self.gtfs, mode="r")

class RTParser:
    def __init__(self, gtfs_source, debug):
        self.container = None
        self.debug = bool(debug)

        self.trip_vehicle = {}
        self.trip_delays = {}
        self.vehicles = {}

        print("\033[1A\033[K" + "Attepmpting to load GTFS")
        self.gtfs = GdanskData(gtfs_source, bool(debug))
        self.gtfs.get_gtfs()
        self.gtfs.load_gtfs()

    def init_container(self):
        print("\033[1A\033[K" + "RT: Creating a new, empty gtfs-rt FeedMessage()")

        self.container = gtfs_rt.FeedMessage()
        self.container.header.gtfs_realtime_version = "2.0"
        self.container.header.incrementality = 0
        self.container.header.timestamp = round(datetime.today().timestamp())

    def alerts(self):
        print("\033[1A\033[K" + "RT: Loading and creating FeedEntities for ZTM Gdańsk alerts")

        req = requests.get("http://ztm.gda.pl/rozklady/download/opendata_out/bsk.json", verify=False)
        req.raise_for_status()
        req = req.json()

        for idx, alert in enumerate(req.get("komunikaty", [])):
            entity = self.container.entity.add()
            entity.id = "ALERT_{}".format(idx)

            entity.alert.header_text.translation.add().text = alert["tytul"]
            entity.alert.description_text.translation.add().text = no_html(alert["tresc"])

            peroid = entity.alert.active_period.add()
            peroid.start = round(datetime.strptime(alert["data_rozpoczecia"], "%Y-%m-%d %H:%M:%S").timestamp())
            peroid.end = round(datetime.strptime(alert["data_zakonczenia"], "%Y-%m-%d %H:%M:%S").timestamp())

    def load_delays(self):
        # Load trip delays to memory
        print("\033[1A\033[K" + "RT: Loading Tristar per-stop delays")

        req = requests.get("http://ckan2.multimediagdansk.pl/delays")
        req.raise_for_status()
        req = req.json()

        for stop_id, delay_container in req.items():
            for delay in delay_container["delay"]:
                route_id = str(delay["routeId"])
                time = delay["theoreticalTime"]

                # self.stop_trips[stop][route][static_time] = row["trip_id"]
                trip_id = self.gtfs.stop_trips.get(stop_id, {})\
                                              .get(route_id, {})\
                                              .get(time, None)\

                if not trip_id:
                    if self.debug: print("\033[1m" + "No matching trip_id for S {}, R {}, T {}".format(stop_id, route_id, time) + "\033[0m")
                    continue

                # Fix timestamp
                update_time = datetime.strptime(delay["timestamp"], "%H:%M:%S").time()

                if update_time > (datetime.today() + timedelta(minutes=2)).time():
                    update_time = datetime.combine(
                        date=(datetime.today().date() - timedelta(days=1)),
                        time=update_time
                    )

                else:
                    update_time = datetime.combine(
                        date=datetime.today().date(),
                        time=update_time
                    )

                # Add some values
                self.trip_vehicle[trip_id] = str(delay["vehicleId"])

                if trip_id not in self.trip_delays:
                    self.trip_delays[trip_id] = []

                # Fix estimates
                estimate = delay["estimatedTime"]
                if any([re.match(r"2\d:\d\d", i["estimated"]) for i in self.trip_delays[trip_id]]):
                    estimate_h, estimate_m = map(int, estimate.split(":"))
                    estimate_h += 24
                    estimate = "{:0>2d}:{:0>2d}".format(estimate_h, estimate_m)

                    del estimate_h, estimate_m

                self.trip_delays[trip_id].append({
                    "stop_id": stop_id,
                    "timestamp": round(update_time.timestamp()),
                    "delay": delay["delayInSeconds"],
                    "estimated": estimate,
                })

    def load_vehicles(self):
        print("\033[1A\033[K" + "RT: Loading Tristar vehicle positions")

        req = requests.get("http://ckan2.multimediagdansk.pl/gpsPositions")
        req.raise_for_status()
        req = req.json()

        for veh in req["Vehicles"]:
            if not veh["Line"] or veh["GPSQuality"] != 3: continue

            veh_id = str(veh["VehicleId"])
            tstamp = datetime.strptime(veh["DataGenerated"], "%Y-%m-%d %H:%M:%S")

            self.vehicles[veh_id] = {
                "code": str(veh["VehicleCode"]),
                "timestamp": round(tstamp.timestamp()),
                "speed": veh["Speed"] / 3.6, # Convert km/h → m/s
                "lat": veh["Lat"],
                "lon": veh["Lon"]
            }

    def updates(self):
        print("\033[1A\033[K" + "RT: Creating FeedEnities for trip delays and vehicle positions")

        for idx, (trip_id, stop_updates) in enumerate(self.trip_delays.items()):
            entity = self.container.entity.add()
            entity.id = "UPDATE_{}".format(idx)

            stop_updates = sorted(stop_updates, key=lambda i: i["estimated"])

            entity.trip_update.trip.trip_id = trip_id
            entity.trip_update.timestamp = min([i["timestamp"] for i in stop_updates])
            entity.trip_update.delay = stop_updates[0]["delay"]

            for stop_update in stop_updates:
                update_event = entity.trip_update.stop_time_update.add()
                update_event.stop_id = stop_update["stop_id"]
                update_event.arrival.delay = stop_update["delay"]

            if self.trip_vehicle[trip_id] in self.vehicles:
                veh_data = self.vehicles[self.trip_vehicle[trip_id]]

                veh_entity = self.container.entity.add()
                veh_entity.id = "VEHICLE_{}".format(idx)

                veh_entity.vehicle.trip.trip_id =  trip_id

                veh_entity.vehicle.position.latitude = veh_data["lat"]
                veh_entity.vehicle.position.longitude = veh_data["lon"]
                veh_entity.vehicle.position.speed = veh_data["speed"]

                veh_entity.vehicle.vehicle.id = self.trip_vehicle[trip_id]
                veh_entity.vehicle.vehicle.label = veh_data["code"]

                veh_entity.vehicle.timestamp = veh_data["timestamp"]

    def dump_container(self, target="gtfs-rt.pb", for_humans=False):
        print("\033[1A\033[K" + "RT: Dumping FeedMessage to {} ({})".format(
            target, "human-readable" if for_humans else "binary"
        ))

        if for_humans:
            with open(target, "w", encoding="utf8") as f:
                f.write(str(self.container))
        else:
            with open(target, "wb") as f:
                f.write(self.container.SerializeToString())


    def create(self, target, for_humans):
        self.init_container()
        self.alerts()
        self.load_delays()
        self.load_vehicles()
        self.updates()
        self.dump_container(target, for_humans)

    @classmethod
    def loop(cls, gtfs_source="gtfs.zip", debug=False, peroid=30, gtfs_check_peroid=1800, target="gtfs-rt.pb", for_humans=False):
        self = cls(gtfs_source, debug)

        rt_update = datetime.min
        gtfs_update = datetime.today()

        try:
            while True:
                rt_update = datetime.today()

                if (datetime.today() - gtfs_update).total_seconds() > gtfs_check_peroid:
                    print("\033[1A\033[K" + "Checking if new GTFS is available")
                    if self.gtfs.new_gtfs_available():
                        print("\033[1A\033[K" + "Attepmpting to load GTFS")
                        self.gtfs.get_gtfs()
                        self.gtfs.load_gtfs()

                print("\033[1A\033[K" + "Creating GTFS-Realtime")
                self.create(target, for_humans)

                # Sleep by `peroid` seconds minus what it took to create GTFS-RT file
                sleep_time = peroid - (datetime.today() - rt_update).total_seconds()
                if sleep_time < 0: sleep_time = 15

                print("\033[1A\033[K" + "Sleeping until " +
                    (datetime.today() + timedelta(seconds=sleep_time)).strftime("%H:%M:%S"),
                    end="\n\n"
                )

                time.sleep(sleep_time)

        finally:
            self.gtfs.gtfs.close()
            self.gtfs.arch.close()

if __name__ == "__main__":
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-o", "--output-file", default="gtfs-rt.pb", required=False, metavar="(path)", dest="target", help="destination of gtfs-realtime file (defualt: gtfs-rt.pb)")

    argprs.add_argument("--gtfs", default="https://mkuran.pl/feed/tristar/tristar-latest.zip", required=False, metavar="(path or url)", help="path/URL to the Tristar GTFS file")
    argprs.add_argument("--readable", action="store_true", required=False, help="output data to a human-readable protobuff instead of binary one")
    argprs.add_argument("--debug", action="store_true", required=False, help="do some more printing when there are issues with encountered data")

    argprs.add_argument("-l", "--loop", action="store_true", required=False, help="run the script in a loop - autmoatically update the taget-file")
    argprs.add_argument("-p", "--peroid", default=30, type=int, required=False, metavar="(seconds)", help="how often should the target-file should be updated")
    argprs.add_argument("--gtfs-check-peroid", default=1800, type=int, required=False, metavar="(seconds)", help="how often should the script check if gtfs file has changed")

    args = argprs.parse_args()

    print("Starting TristarGTFS Realtime in {} mode ({})".format(
            "loop" if args.loop else "single", "readable" if args.readable else "binary"
        ), end="\n\n"
    )

    if args.loop:
        RTParser.loop(
            args.gtfs, args.debug, args.peroid,
            args.gtfs_check_peroid, args.target, args.readable
        )

    else:
        rt = RTParser(args.gtfs, args.debug)
        try:
            rt.create(args.target, args.readable)
        finally:
            rt.gtfs.gtfs.close()
            rt.gtfs.arch.close()
