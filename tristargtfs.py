from contextlib import contextmanager
from itertools import chain
from warnings import warn
from tempfile import TemporaryFile, NamedTemporaryFile
from datetime import datetime, timedelta
import pyroutelib3
import argparse
import requests
import zipfile
import signal
import math
import json
import time
import zlib
import rdp
import csv
import re
import io
import os

__title__ = "TristarGTFS"
__author__ = "Mikołaj Kuranowski"
__email__ = "mikolaj@mkuran.pl"
__license__ = "MIT"

@contextmanager
def time_limit(sec):
    "Time limter based on https://gist.github.com/Rabbit52/7449101"
    def handler(x, y): raise TimeoutError
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(sec)
    try: yield
    finally: signal.alarm(0)

def csv_escape(txt):
    return '"' + txt.replace('"', '""') + '"'

def gdansk_route_names():
    req = requests.get("https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/22313c56-5acf-41c7-a5fd-dc5dc72b3851/download/routes.json")
    req.raise_for_status()
    all_routes = req.json()

    route_names = {"F5": "Żabi Kruk - Westerplatte - Brzeźno", "F6": "Targ Rybny - Sobieszewo"}

    for routes in map(lambda i: all_routes[i]["routes"], sorted(all_routes.keys())):
        for route in routes:
            if route["routeShortName"] in route_names:
                continue
            else:
                route_names[route["routeShortName"]] = route["routeLongName"]

    return route_names

def route_color(agency, traction):
    # Colors from mzkzg.org map
    if agency == "1":
        if traction in {"0", "900"}:
            return "D4151D", "FFFFFF"
        elif traction == {"4", "1200"}:
            return "6CF1FA", "000000"
        else:
            return "FC7DAB", "000000"

    else:
        if traction in {"11", "800"}:
            return "91BE40", "000000"
        else:
            return "009CDA", "FFFFFF"


class TristarGtfs:
    def __init__(self, publisher_name=None, publisher_url=None):
        self.data_download = None
        self.publisher_name = publisher_name
        self.publisher_url = publisher_url

        self.gdansk = None
        self.gdynia = None

        self.gdansk_file = TemporaryFile()
        self.gdynia_file = TemporaryFile()

        # self.stop_merge_table = {}

        self.active_services = set()
        self.active_shapes = set()
        self.active_trips = set()

        self.download()

    def download(self):
        print("\033[1A\033[K" + "Downloading Gdansk GTFS")
        self.data_download = datetime.today()

        req = requests.get("https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip")
        req.raise_for_status()
        self.gdansk_file.write(req.content)
        self.gdansk_file.seek(0)
        self.gdansk = zipfile.ZipFile(self.gdansk_file, mode="r")

        print("\033[1A\033[K" + "Downloading Gdynia GTFS")
        req = requests.get("http://api.zdiz.gdynia.pl/pt/gtfs.zip")
        req.raise_for_status()
        self.gdynia_file.write(req.content)
        self.gdynia_file.seek(0)
        self.gdynia = zipfile.ZipFile(self.gdynia_file, mode="r")

    def static_files(self):
        print("\033[1A\033[K" + "Creating agency.txt, feed_info.txt and attributions.txt")
        version = self.data_download.strftime("%Y-%m-%d %H:%M:%S")

        # Agency
        file = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="\r\n")
        file.write('agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url\n')
        file.write('1,"ZTM Gdańsk","https://ztm.gda.pl/",Europe/Warsaw,pl,+48 58 520 57 75,"https://ztm.gda.pl/hmvc/index.php/test/wiecej/taryfa"\n')
        file.write('2,"ZKM Gdynia","https://zkmgdynia.pl/",Europe/Warsaw,pl,+48 695 174 194,"https://zkmgdynia.pl/bilety-jednorazowe-zkm-w-gdyni-i-metropolitalne-mzkzg"\n')
        file.close()

        # Feed Info
        if self.publisher_name and self.publisher_url:
            file = open("gtfs/feed_info.txt", mode="w", encoding="utf8", newline="\r\n")
            file.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n")
            file.write(f"{csv_escape(self.publisher_name)},{csv_escape(self.publisher_url)},"
                       f"pl,{version}\n")
            file.close()

        # Attributions
        file = open("gtfs/attributions.txt", mode="w", encoding="utf8", newline="\r\n")
        file.write("attribution_id,agency_id,organization_name,is_producer,is_operator,"
                       "is_authority,is_data_source,attribution_url\n")

        file.write(f'1,1,"Based on data by: Zarząd Transportu Miejskiego w Gdańsku '
                   f'(retrieved {version})",0,0,1,1,"http://www.ztm.gda.pl/otwarty_ztm"\n')

        file.write(f'2,2,"Based on data by: Zarząd Dróg i Zieleni w Gdyni (retrieved {version}, '
                   'agency not responsible for this dataset)",0,0,1,1,'
                    '"http://otwartedane.gdynia.pl/pl/dataset/informacje-o-rozkladach-jazdy-i-lokalizacji-przystankow"\n')

        file.close()

    @staticmethod
    def compress(target="gtfs.zip"):
        print("\033[1A\033[K" + "Compressing to " + target)
        with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as arch:
            for file in os.listdir("gtfs"):
                if not file.endswith(".txt"): continue
                arch.write(os.path.join("gtfs", file), file)

    def merge_stops(self):
        file = open("gtfs/stops.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, ["stop_id", "stop_name", "stop_lat", "stop_lon"], extrasaction="ignore")
        writer.writeheader()

        # Load merge table
        # The merge table maps to some non-existing stops and generally causes problems

        #print("\033[1A\033[K" + "Loading stop merge table")

        #req = requests.get("https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/f8a5bedb-7925-40c9-8d66-dbbc830939b1/download/przystanki_wspolnegda_gdy.csv")
        #req.raise_for_status()
        #req.encoding = "utf-8"

        #for row in csv.DictReader(io.StringIO(req.text)):
        #    source, target = None, None

        #    if row["mapped_organization_id"] == "2":
        #        if int(row["mapped_gmv_short_name"]) < 30000: source = str(30000 + int(row["mapped_gmv_short_name"]))
        #        else: source = row["mapped_gmv_short_name"]
        #    else:
        #        source = row["mapped_gmv_short_name"]

        #    if row["main_organization_id"] == "2":
        #        if int(row["main_gmv_short_name"]) < 30000: target = str(30000 + int(row["main_gmv_short_name"]))
        #        else: target = row["main_gmv_short_name"]
        #    else:
        #        target = row["main_gmv_short_name"]

        #    self.stop_merge_table[source] = target

        print("\033[1A\033[K" + "Merging Gdańsk stops")

        with self.gdansk.open("stops.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                #if row["stop_id"] in self.stop_merge_table: continue

                # Strip Gdynia from stop names — that's how it's printed on maps, see mzkzg.org
                if row["stop_name"].startswith("Gdynia"):
                    row["stop_name"] = row["stop_name"][7:]

                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia stops")

        with self.gdynia.open("stops.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                #if int(row["stop_id"]) < 30000: source = str(30000 + int(row["stop_id"]))
                #else: source = row["stop_id"]

                #if row["stop_id"] in self.stop_merge_table: continue
                writer.writerow(row)

        file.close()

    def merge_routes(self):
        file = open("gtfs/routes.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, [
                "agency_id", "route_id", "route_short_name", "route_long_name",
                "route_type", "route_color", "route_text_color"
            ], extrasaction="ignore"
        )
        writer.writeheader()

        print("\033[1A\033[K" + "Downloading route_long_names")

        route_names = {}
        route_names.update(gdansk_route_names())

        print("\033[1A\033[K" + "Merging Gdańsk routes")

        with self.gdansk.open("routes.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["agency_id"] = "1"
                row["route_id"] = "1:" + row["route_id"]

                row["route_short_name"] = row["route_short_name"].strip()

                row["route_long_name"] = route_names.get(row["route_short_name"], "")
                row["route_color"], row["route_text_color"] = route_color(row["agency_id"], row["route_type"])

                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia routes")

        with self.gdynia.open("routes.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["agency_id"] = "2"
                row["route_id"] = "2:" + row["route_id"]

                row["route_short_name"] = row["route_short_name"].strip()
                row["route_long_name"] = row["route_long_name"].replace('""', '"')

                row["route_color"], row["route_text_color"] = route_color(row["agency_id"], row["route_type"])

                writer.writerow(row)

    def merge_dates(self):
        file = open("gtfs/calendar_dates.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, ["date", "service_id", "exception_type"], extrasaction="ignore")
        writer.writeheader()

        gdansk_dates, gdynia_dates = [], []

        print("\033[1A\033[K" + "Loading Gdańsk services")

        with self.gdansk.open("calendar_dates.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                if row["exception_type"] != "1": continue
                row["service_id"] = "1:" + row["service_id"]
                row["date"] = datetime.strptime(row["date"], "%Y%m%d").date()
                gdansk_dates.append(row)

        print("\033[1A\033[K" + "Loading Gdynia services")

        with self.gdynia.open("calendar_dates.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:

                if row["exception_type"] != "1": continue

                row["service_id"] = "2:" + row["service_id"]
                row["date"] = datetime.strptime(row["date"], "%Y%m%d").date()
                gdynia_dates.append(row)

        print("\033[1A\033[K" + "Exporting all services")

        # Find common start & end date for calendars
        start_date = max(min([i["date"] for i in gdansk_dates]), min([i["date"] for i in gdynia_dates]))
        end_date = min(max([i["date"] for i in gdansk_dates]), max([i["date"] for i in gdynia_dates]))

        services_on_date = {}
        for service_date in chain(gdansk_dates, gdynia_dates):
            if service_date["date"] > end_date: continue
            if service_date["date"] < start_date: continue

            date_str = service_date["date"].strftime("%Y%m%d")
            if date_str not in services_on_date: services_on_date[date_str] = set()

            self.active_services.add(service_date["service_id"])
            services_on_date[date_str].add(service_date["service_id"])

        while start_date <= end_date:
            date_str = start_date.strftime("%Y%m%d")
            services = sorted(services_on_date[date_str])

            for service in services:
                writer.writerow({
                    "date": date_str,
                    "service_id": service,
                    "exception_type": "1"
                })

            start_date += timedelta(days=1)

        file.close()

    def merge_times(self):
        file = open("gtfs/stop_times.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, [
                "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"
            ], extrasaction="ignore"
        )
        writer.writeheader()

        print("\033[1A\033[K" + "Merging Gdańsk stop_times")

        with self.gdansk.open("stop_times.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["trip_id"] = "1:" + row["trip_id"]
                # row["stop_id"] = self.stop_merge_table.get(row["stop_id"], row["stop_id"])

                if row["trip_id"] not in self.active_trips:
                    continue

                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia stop_times")

        with self.gdynia.open("stop_times.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["trip_id"] = "2:" + row["trip_id"]
                # row["stop_id"] = self.stop_merge_table.get(row["stop_id"], row["stop_id"])

                if row["trip_id"] not in self.active_trips:
                    continue

                writer.writerow(row)

        file.close()

    def merge_shapes(self):
        file = open("gtfs/shapes.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(
            file,
            ["shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"],
            extrasaction="ignore")
        writer.writeheader()

        print("\033[1A\033[K" + "Merging Gdańsk shapes")

        with self.gdansk.open("shapes.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["shape_id"] = "1:" + row["shape_id"]
                if row["shape_id"] not in self.active_shapes:
                    continue

                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia shapes")

        with self.gdynia.open("shapes.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["shape_id"] = "2:" + row["shape_id"]
                if row["shape_id"] not in self.active_shapes:
                    continue

                writer.writerow(row)

        file.close()

    def merge_trips(self):
        file = open("gtfs/trips.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(
            file,
            ["route_id", "service_id", "trip_id", "trip_headsign",
             "direction_id", "shape_id", "wheelchair_accessible"]
            , extrasaction="ignore")
        writer.writeheader()

        print("\033[1A\033[K" + "Merging Gdańsk trips")

        with self.gdansk.open("trips.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["route_id"] = "1:" + row["route_id"]
                row["service_id"] = "1:" + row["service_id"]
                row["trip_id"] = "1:" + row["trip_id"]
                row["shape_id"] = "1:" + row["shape_id"]

                if row["service_id"] not in self.active_services:
                    continue

                self.active_trips.add(row["trip_id"])
                self.active_shapes.add(row["shape_id"])
                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia trips")

        with self.gdynia.open("trips.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["route_id"] = "2:" + row["route_id"]
                row["service_id"] = "2:" + row["service_id"]
                row["trip_id"] = "2:" + row["trip_id"]
                row["shape_id"] = "2:" + row["shape_id"]

                if row["service_id"] not in self.active_services:
                    continue

                self.active_trips.add(row["trip_id"])
                self.active_shapes.add(row["shape_id"])
                writer.writerow(row)

        file.close()

    @classmethod
    def create(cls, target="gtfs.zip", publisher_name=None, publisher_url=None):
        print("Starting TristarGTFS")

        for file in os.scandir("gtfs"):
            os.remove(file.path)

        self = cls(publisher_name, publisher_url)

        self.static_files()

        self.merge_routes()
        self.merge_stops()
        self.merge_dates()
        self.merge_trips()
        self.merge_shapes()
        self.merge_times()

        self.compress(target)


if __name__ == "__main__":
    st = time.time()
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-o", "--output-file", default="gtfs.zip", required=False, metavar="(path)", dest="target", help="destination of the gtfs file (defualt: gtfs.zip)")
    argprs.add_argument("-pn", "--publisher-name", required=False, metavar="NAME", dest="publisher_name", help="value of feed_publisher_name")
    argprs.add_argument("-pu", "--publisher-url", required=False, metavar="URL", dest="publisher_url", help="value of feed_publisher_url")

    args = argprs.parse_args()

    print(r"""
  _______   _     _              _____ _______ ______ _____
 |__   __| (_)   | |            / ____|__   __|  ____/ ____|
    | |_ __ _ ___| |_ __ _ _ __| |  __   | |  | |__ | (___
    | | '__| / __| __/ _` | '__| | |_ |  | |  |  __| \___ \
    | | |  | \__ \ || (_| | |  | |__| |  | |  | |    ____) |
    |_|_|  |_|___/\__\__,_|_|   \_____|  |_|  |_|   |_____/
    """)

    TristarGtfs.create(args.target, args.publisher_name, args.publisher_url)

    print("=== Done! In %s sec. ===" % round(time.time() - st, 3))
