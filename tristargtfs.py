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

def gdansk_route_names():
    req = requests.get("https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/22313c56-5acf-41c7-a5fd-dc5dc72b3851/download/routes.json")
    req.raise_for_status()
    all_routes = req.json()

    route_names = {"F5": "Żabi Kruk — Westerplatte — Brzeźno", "F6": "Targ Rybny — Sobieszewo"}

    for routes in map(lambda i: all_routes[i]["routes"], sorted(all_routes.keys())):
        for route in routes:
            if route["routeShortName"] in route_names: continue
            else: route_names[route["routeShortName"]] = route["routeLongName"].replace(" - ", " — ")

    return route_names

def gdynia_route_names():
    req = requests.get("https://zkmgdynia.pl/api/rozklady/1.0.0/pl/list")
    req.raise_for_status()
    req.encoding = "utf-8"
    req = req.json()

    route_names = {"F": "Gdynia Plac Kaszubski — Gdynia Terminal Promowy"}

    for route_data in chain(req["resultData"]["planned"]["items"]["trol"]["items"],\
                            req["resultData"]["planned"]["items"]["bus"]["items"]):

        route = route_data["title"].rstrip("*")

        if route in route_names:
            continue
        elif len(route_data["hint"]) < 1:
            warn("Route {} has no long name in ZKM API".format(route))
            continue

        else:
            name = route_data["hint"][0]

        if "<" not in name and ">" not in name and "-" in name:
            name_pattern = [name.split("-")[0], "-", name.split("-")[1]]
        else:
            name_pattern = list(map(str.strip, re.split(r"([<>-]{2,})", name)))

        for idx, name_part in enumerate(name_pattern):
            # Arrows
            if name_part == "<->" or name_part == "-":
                name_pattern[idx] = "—"

            elif name_part == "->":
                name_pattern[idx] = "→"

            elif name_part == "<-":
                name_pattern[idx] = "←"
            # Names
            else:

                # Get rid of brackets
                if "(" in name_part:
                    name_part = name_part.split("(")[0].strip()

                # Avoid something like "Gdynia: Gdynia Dworzec Gł. PKP"
                if ":" in name_part:
                    town_name, stop_name = name_part.split(":")

                    if set(town_name.split()).isdisjoint(stop_name.split()):
                        name_pattern[idx] = town_name + " " + stop_name

                    else:
                        name_pattern[idx] = stop_name

        route_names[route] = " ".join(name_pattern).replace("  ", " ").strip()

    return route_names

def route_color(agency, traction):
    # Colors from mzkzg.org map
    if agency == "1":
        if traction == "0": return "D4151D", "FFFFFF"
        elif traction == "4": return "6CF1FA", "000000"
        else: return "FC7DAB", "000000"

    else:
        if traction == "800": return "91BE40", "000000"
        else: return "009CDA", "FFFFFF"

class Shaper:
    def __init__(self):
        self.stops = {}
        self.generated = {}

        self.enum = 0

        self.create_router()

    def create_router(self):
        print("\033[1A\033[K" + "Creating shape generator")
        request = requests.get(r"https://overpass-api.de/api/interpreter/?data=%5Bbbox%3A54.32%2C18.21%2C54.65%2C18.61%5D%5Bout%3Axml%5D%3B%0A(%0A%20way%5B%22highway%22%3D%22motorway%22%5D%3B%0A%20way%5B%22highway%22%3D%22motorway_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk%22%5D%3B%0A%20way%5B%22highway%22%3D%22trunk_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary%22%5D%3B%0A%20way%5B%22highway%22%3D%22primary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary%22%5D%3B%0A%20way%5B%22highway%22%3D%22secondary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary%22%5D%3B%0A%20way%5B%22highway%22%3D%22tertiary_link%22%5D%3B%0A%20way%5B%22highway%22%3D%22unclassified%22%5D%3B%0A%20way%5B%22highway%22%3D%22minor%22%5D%3B%0A%20way%5B%22highway%22%3D%22residential%22%5D%3B%0A%20way%5B%22highway%22%3D%22living_street%22%5D%3B%0A%20way%5B%22highway%22%3D%22service%22%5D%3B%0A)%3B%0A%3E-%3E.n%3B%0A%3C-%3E.r%3B%0A(._%3B.n%3B.r%3B)%3B%0Aout%3B%0A")

        temp_xml = NamedTemporaryFile(delete=False)
        temp_xml.write(request.content)
        temp_xml.seek(0)

        self.router = pyroutelib3.Router("bus", temp_xml.name)

        temp_xml.close()

    def rotue_between_stops(self, start_stop, end_stop):
        # Find nodes
        start_lat, start_lon = map(float, self.stops[start_stop])
        end_lat, end_lon = map(float, self.stops[end_stop])
        start = self.router.findNode(start_lat, start_lon)
        end = self.router.findNode(end_lat, end_lon)

        # Do route

        # SafetyCheck - start and end nodes have to be defined
        if start and end:
            try:
                with time_limit(10):
                    status, route = self.router.doRoute(start, end)
            except TimeoutError:
                status, route = "timeout", []

            route_points = list(map(self.router.nodeLatLon, route))

            # SafetyCheck - route has to have at least 2 nodes
            if status == "success" and len(route_points) <= 1:
                status = "to_few_nodes_({d})".format(len(route))

            # Apply rdp algorithm
            route_points = rdp.rdp(route_points, epsilon=0.000006)

        else:
            start, end = math.nan, math.nan
            dist_ratio = math.nan
            status = "no_nodes_found"

        # If we failed, catch some more info on why
        if status != "success":

            ### DEBUG-SHAPES ###
            if not os.path.exists("shape-errors/{}-{}.json".format(start_stop, end_stop)):
                with open("shape-errors/{}-{}.json".format(start_stop, end_stop), "w") as f:
                    json.dump(
                        {"start": start_stop, "end": end_stop,
                         "start_node": start, "end_node": end,
                         "start_pos": [start_lat, start_lon],
                         "end_pod": [end_lat, end_lon],
                         "error": status
                        }, f, indent=2
                    )

            route_points = [[start_lat, start_lon], [end_lat, end_lon]]

        return route_points

    def get(self, stops):
        stops_hashable = "-".join(stops)

        if stops_hashable in self.generated:
            return self.generated[stops_hashable]

        self.enum += 1
        pattern_id = "2:" + str(self.enum)

        pt_seq = 0

        routes = []
        for i in range(1, len(stops)):
            routes.append(self.rotue_between_stops(stops[i-1], stops[i]))

        for x in range(len(routes)):
            leg = routes[x]

            # We always ignore first point of route leg [it's the same as next routes first point],
            # but this would make the script ignore the very first point (corresponding to start stop)
            if x == 0:
                pt_seq += 1
                self.writer.writerow([pattern_id, pt_seq, leg[0][0], leg[0][1]])

            # Output points of leg
            for y in range(1, len(leg)):
                pt_seq += 1
                self.writer.writerow([pattern_id, pt_seq, leg[y][0], leg[y][1]])

        self.generated[stops_hashable] = pattern_id
        return pattern_id

    def open(self):
        self.file = open("gtfs/shapes.txt", "w", encoding="utf-8", newline="")
        self.writer = csv.writer(self.file)
        self.writer.writerow(["shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"])

    def close(self):
        self.file.close()

class TristarGtfs:
    def __init__(self, shapes):
        self.shapes = shapes
        self.shape_gen = Shaper() if shapes else None

        self.gdansk = None
        self.gdynia = None

        self.gdansk_file = TemporaryFile()
        self.gdynia_file = TemporaryFile()

        #self.stop_merge_table = {}

        self.active_services = set()
        self.active_trips = set()

        self.download()

    def download(self):
        print("\033[1A\033[K" + "Downloading Gdansk GTFS")
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

    def gdynia_times(self):
        gdynia_trips = {}

        with self.gdynia.open("stop_times.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["trip_id"] = "2:" + row["trip_id"]
                #row["stop_id"] = self.stop_merge_table.get(row["stop_id"], row["stop_id"])

                if row["trip_id"] not in gdynia_trips: gdynia_trips[row["trip_id"]] = []

                gdynia_trips[row["trip_id"]].append({
                    "stop": row["stop_id"],
                    "order": int(row["stop_sequence"])
                })

        return gdynia_trips

    def static_files(self):
        print("\033[1A\033[K" + "Creating feed_info.txt and agency.txt")

        file = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="\r\n")
        file.write('agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url\n')
        file.write('1,"ZTM Gdańsk","https://ztm.gda.pl/",Europe/Warsaw,pl,+48 58 520 57 75,"https://ztm.gda.pl/hmvc/index.php/test/wiecej/taryfa"\n')
        file.write('2,"ZKM Gdynia","https://zkmgdynia.pl/",Europe/Warsaw,pl,+48 695 174 194,"https://zkmgdynia.pl/bilety-jednorazowe-zkm-w-gdyni-i-metropolitalne-mzkzg"\n')
        file.close()

        file = open("gtfs/feed_info.txt", mode="w", encoding="utf8", newline="\r\n")
        file.write('feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n')

        if self.shapes:
            file.write(",".join([
                '"Data: Zarząd Transportu Miejskiego w Gdańsku, Zarząd Komunikacji Miejskiej w Gdyni (both under CC-BY license) and ZKM Gdynia shapes from © OpenStreetMap contributors (under ODbL license) with modifications from TristarGTFS script"',
                '"https://github.com/MKuranowski/TristarGTFS"', "pl", datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            ]))

        else:
            file.write(",".join([
                '"Data: Zarząd Transportu Miejskiego w Gdańsku and Zarząd Komunikacji Miejskiej w Gdyni (both under CC-BY license) with modifications from TristarGTFS script"',
                '"https://github.com/MKuranowski/TristarGTFS"', "pl", datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            ]))

        file.close()

    @staticmethod
    def compress(target="gtfs.zip"):
        print("\033[1A\033[K" + "Loading Gdynia times for shape generations")
        with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as arch:
            for file in os.listdir("gtfs"):
                if not file.endswith(".txt"): continue
                arch.write(os.path.join("gtfs", file), file)

    def merge_stops(self):
        file = open("gtfs/stops.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, ["stop_id", "stop_name", "stop_lat", "stop_lon"], extrasaction="ignore")
        writer.writeheader()

        # Load merge table
        print("\033[1A\033[K" + "Loading stop merge table")

        req = requests.get("https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/f8a5bedb-7925-40c9-8d66-dbbc830939b1/download/przystanki_wspolnegda_gdy.csv")
        req.raise_for_status()
        req.encoding = "utf-8"

        for row in csv.DictReader(io.StringIO(req.text)):
            src, to = None, None

            if row["mapped_organization_id"] == "2":
                if int(row["mapped_gmv_short_name"]) < 30000: source = str(30000 + int(row["mapped_gmv_short_name"]))
                else: source = row["mapped_gmv_short_name"]
            else:
                source = row["mapped_gmv_short_name"]

            if row["main_organization_id"] == "2":
                if int(row["main_gmv_short_name"]) < 30000: target = str(30000 + int(row["main_gmv_short_name"]))
                else: target = row["main_gmv_short_name"]
            else:
                target = row["main_gmv_short_name"]

            #self.stop_merge_table[source] = target

        print("\033[1A\033[K" + "Merging Gdańsk stops")

        with self.gdansk.open("stops.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                #if row["stop_id"] in self.stop_merge_table: continue

                # Strip Gdynia from stop names — that's how it's printed on maps, see mzkzg.org
                if row["stop_name"].startswith("Gdynia"):
                    row["stop_name"] = row["stop_name"][7:]

                if self.shapes:
                    self.shape_gen.stops[row["stop_id"]] = (row["stop_lat"], row["stop_lon"])
                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia stops")

        with self.gdynia.open("stops.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                if int(row["stop_id"]) < 30000: source = str(30000 + int(row["stop_id"]))
                else: source = row["stop_id"]

                #if row["stop_id"] in self.stop_merge_table: continue

                if self.shapes:
                    self.shape_gen.stops[row["stop_id"]] = (row["stop_lat"], row["stop_lon"])
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
        route_names.update(gdynia_route_names())

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

                row["route_long_name"] = route_names.get(row["route_short_name"], "")

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
                #row["stop_id"] = self.stop_merge_table.get(row["stop_id"], row["stop_id"])

                if row["trip_id"] not in self.active_trips: continue

                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia stop_times")

        with self.gdynia.open("stop_times.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["trip_id"] = "2:" + row["trip_id"]
                #row["stop_id"] = self.stop_merge_table.get(row["stop_id"], row["stop_id"])

                if row["trip_id"] not in self.active_trips: continue

                writer.writerow(row)

        file.close()

    def merge_trips_shapes(self):
        ## SHAPES: Load Gdynia trips for shape generation and copy Gdańsk shapes
        if self.shapes:
            print("\033[1A\033[K" + "Loading Gdynia times for shape generations")
            gdynia_trips = self.gdynia_times()
            fields = ["route_id", "service_id", "trip_id", "trip_headsign",
                "direction_id", "shape_id", "wheelchair_accessible"
            ]

            print("\033[1A\033[K" + "Merging Gdańsk shapes")
            self.shape_gen.open()
            with self.gdansk.open("shapes.txt") as buffer:
                reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
                for row in reader:
                    self.shape_gen.writer.writerow([
                        "1:" + row["shape_id"],
                        row["shape_pt_sequence"],
                        row["shape_pt_lat"],
                        row["shape_pt_lon"]
                    ])

        else:
            gdynia_trips = {}
            fields = ["route_id", "service_id", "trip_id",
                "trip_headsign", "direction_id", "wheelchair_accessible"
            ]

        file = open("gtfs/trips.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, fields, extrasaction="ignore")
        writer.writeheader()

        print("\033[1A\033[K" + "Merging Gdańsk trips")

        with self.gdansk.open("trips.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["route_id"] = "1:" + row["route_id"]
                row["service_id"] = "1:" + row["service_id"]
                row["trip_id"] = "1:" + row["trip_id"]
                row["shape_id"] = "1:" + row["shape_id"]

                if row["service_id"] not in self.active_services: continue

                self.active_trips.add(row["trip_id"])
                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia trips")

        with self.gdynia.open("trips.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                row["route_id"] = "2:" + row["route_id"]
                row["service_id"] = "2:" + row["service_id"]
                row["trip_id"] = "2:" + row["trip_id"]

                if row["service_id"] not in self.active_services: continue

                if self.shapes:
                    print("\033[1A\033[K" + "Generating shape for Gdynia trip", row["trip_id"])
                    row["shape_id"] = self.shape_gen.get(
                        [i["stop"] for i in sorted(gdynia_trips[row["trip_id"]], key=lambda i: i["order"])]
                    )
                    print("\033[1A\033[K" + "Merging Gdynia trips")

                self.active_trips.add(row["trip_id"])
                writer.writerow(row)

        if self.shapes: self.shape_gen.close()
        file.close()

    @classmethod
    def create(cls, shapes, target="gtfs.zip"):
        print("Starting TristarGTFS")

        self = cls(shapes)

        self.static_files()

        self.merge_routes()
        self.merge_stops()
        self.merge_dates()
        self.merge_trips_shapes()
        self.merge_times()

        self.compress(target)

if __name__ == "__main__":
    st = time.time()
    argprs = argparse.ArgumentParser()
    argprs.add_argument("-o", "--output-file", default="gtfs.zip", required=False, metavar="(path)", dest="target", help="destination of the gtfs file (defualt: gtfs.zip)")
    argprs.add_argument("-s", "--shapes", action="store_true", required=False, help="generate shapes for ZKM Gdynia and merge with ZTM Gdańsk shapes")

    args = argprs.parse_args()

    print("""
  _______   _     _              _____ _______ ______ _____
 |__   __| (_)   | |            / ____|__   __|  ____/ ____|
    | |_ __ _ ___| |_ __ _ _ __| |  __   | |  | |__ | (___
    | | '__| / __| __/ _` | '__| | |_ |  | |  |  __| \___ \\
    | | |  | \__ \ || (_| | |  | |__| |  | |  | |    ____) |
    |_|_|  |_|___/\__\__,_|_|   \_____|  |_|  |_|   |_____/
    """)

    TristarGtfs.create(args.shapes, args.target)

    print("=== Done! In %s sec. ===" % round(time.time() - st, 3))
