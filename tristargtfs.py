import argparse
import csv
import io
import os
import time
import zipfile
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from itertools import chain
from tempfile import TemporaryFile
from typing import Dict, Generator, List, NamedTuple, Set, Tuple

import requests

__title__ = "TristarGTFS"
__author__ = "Mikołaj Kuranowski"
__license__ = "MIT"
__email__ = "".join(chr(i) for i in [109, 107, 117, 114, 97, 110, 111, 119, 115, 107, 105, 32, 91,
                                     1072, 116, 93, 32, 103, 109, 97, 105, 108, 46, 99, 111, 109])


class ServiceDate(NamedTuple):
    service_id: str
    date: date


def gdansk_route_names() -> Dict[str, str]:
    """Returns a mapping from route_short_name to route_long_name for ZTM Gdańsk,
    as route_long_names aren't included in the main GTFS."""
    req = requests.get("https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/22313c56-5acf-41c7-a5fd-dc5dc72b3851/download/routes.json")  # noqa
    req.raise_for_status()
    all_routes = req.json()

    route_names: Dict[str, str] = {
        "F5": "Żabi Kruk - Westerplatte - Brzeźno",
        "F6": "Targ Rybny - Sobieszewo"}

    for routes in map(lambda i: all_routes[i]["routes"], sorted(all_routes.keys())):
        for route in routes:
            if route["routeShortName"] in route_names:
                continue
            else:
                route_names[route["routeShortName"]] = route["routeLongName"]

    return route_names


def route_color(agency: str, traction: str) -> Tuple[str, str]:
    """Generate route_color and route_text_color given an agency and route_type"""
    # Colors from mzkzg.org map

    # ZTM Gdańsk
    if agency == "1":
        if traction in {"0", "900"}:
            return "D4151D", "FFFFFF"  # Tram
        elif traction == {"4", "1200"}:
            return "6CF1FA", "000000"  # Ferry
        else:
            return "FC7DAB", "000000"  # Bus

    # ZKM Gdańsk
    else:
        if traction in {"11", "800"}:
            return "91BE40", "000000"  # Trolleybus
        else:
            return "009CDA", "FFFFFF"  # Bus


@contextmanager
def csv_reader_from_zip(zip: zipfile.ZipFile, name: str) \
        -> Generator[csv.DictReader[str], None, None]:
    with zip.open(name, mode="r") as raw, \
            io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as wrapped:
        yield csv.DictReader(wrapped)


class TristarGtfs:
    def __init__(self, publisher_name: str = "", publisher_url: str = ""):
        self.publisher_name: str = publisher_name
        self.publisher_url: str = publisher_url
        self.data_download: datetime

        self.gdansk: zipfile.ZipFile
        self.gdynia: zipfile.ZipFile

        self.gdansk_file = TemporaryFile()
        self.gdynia_file = TemporaryFile()

        # self.stop_merge_table = {}

        self.active_services: Set[str] = set()
        self.active_shapes: Set[str] = set()
        self.active_trips: Set[str] = set()

        self.download()

    def download(self):
        print("\033[1A\033[K" + "Downloading Gdansk GTFS")
        self.data_download = datetime.today()

        req = requests.get("https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip")  # noqa
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
        f = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="")
        w = csv.writer(f)
        w.writerow([
            "agency_id", "agency_name", "agency_url", "agency_timezone",
            "agency_lang", "agency_phone", "agency_fare_url"])
        w.writerow([
            "1", "ZTM Gdańsk", "https://ztm.gda.pl/", "Europe/Warsaw",
            "pl", "+48 58 52 44 500", "https://ztm.gda.pl/bilety/ceny-biletow,a,13"])
        w.writerow([
            "2", "ZKM Gdynia", "https://zkmgdynia.pl/", "Europe/Warsaw",
            "pl", "+48 801 174 194",
            "https://zkmgdynia.pl/bilety-jednorazowe-zkm-w-gdyni-i-metropolitalne-mzkzg"])
        f.close()

        # Feed Info
        if self.publisher_name and self.publisher_url:
            f = open("gtfs/feed_info.txt", mode="w", encoding="utf8", newline="")
            w = csv.writer(f)
            w.writerow(["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_version"])
            w.writerow([self.publisher_name, self.publisher_url, "pl", version])
            f.close()

        # Attributions
        f = open("gtfs/attributions.txt", mode="w", encoding="utf8", newline="")
        w = csv.writer(f)
        w.writerow(["attribution_id", "agency_id", "organization_name", "is_producer",
                    "is_operator", "is_authority", "is_data_source", "attribution_url"])
        w.writerow([
            "1", "1",
            f"Based on data by: Zarząd Transportu Miejskiego w Gdańsku (retrieved {version})",
            "0", "1", "1", "1", "https://ckan.multimediagdansk.pl/dataset/tristar"])
        w.writerow([
            "2", "2",
            f"Based on data by: Zarząd Dróg i Zieleni w Gdyni (retrieved {version})",
            "0", "0", "1", "1", "http://otwartedane.gdynia.pl/pl/dataset/informacje-o-rozkladach-jazdy-i-lokalizacji-przystankow"])  # noqa
        f.close()

    @staticmethod
    def compress(target="gtfs.zip"):
        print("\033[1A\033[K" + "Compressing to " + target)
        with zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED) as arch:
            for f in filter(lambda i: i.name.endswith(".txt"), os.scandir("gtfs")):
                arch.write(f.path, f.name)

    def merge_stops(self):
        file = open("gtfs/stops.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, ["stop_id", "stop_name", "stop_lat", "stop_lon"],
                                extrasaction="ignore")
        writer.writeheader()

        # Load merge table
        # The merge table maps to some non-existing stops and generally causes problems

        # print("\033[1A\033[K" + "Loading stop merge table")

        # req = requests.get("https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/f8a5bedb-7925-40c9-8d66-dbbc830939b1/download/przystanki_wspolnegda_gdy.csv")  # noqa
        # req.raise_for_status()
        # req.encoding = "utf-8"

        # for row in csv.DictReader(io.StringIO(req.text)):
        #    source, target = None, None

        #    if row["mapped_organization_id"] == "2":
        #        if int(row["mapped_gmv_short_name"]) < 30000: source = str(30000 + int(row["mapped_gmv_short_name"]))  # noqa
        #        else: source = row["mapped_gmv_short_name"]
        #    else:
        #        source = row["mapped_gmv_short_name"]

        #    if row["main_organization_id"] == "2":
        #        if int(row["main_gmv_short_name"]) < 30000: target = str(30000 + int(row["main_gmv_short_name"]))  # noqa
        #        else: target = row["main_gmv_short_name"]
        #    else:
        #        target = row["main_gmv_short_name"]

        #    self.stop_merge_table[source] = target

        print("\033[1A\033[K" + "Merging Gdańsk stops")

        with self.gdansk.open("stops.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                # if row["stop_id"] in self.stop_merge_table: continue

                # Strip Gdynia from stop names — that's how it's printed on maps, see mzkzg.org
                if row["stop_name"].startswith("Gdynia"):
                    row["stop_name"] = row["stop_name"][7:]

                writer.writerow(row)

        print("\033[1A\033[K" + "Merging Gdynia stops")

        with self.gdynia.open("stops.txt") as buffer:
            reader = csv.DictReader(io.TextIOWrapper(buffer, encoding="utf-8", newline=""))
            for row in reader:
                # if int(row["stop_id"]) < 30000: source = str(30000 + int(row["stop_id"]))
                # else: source = row["stop_id"]

                # if row["stop_id"] in self.stop_merge_table: continue
                writer.writerow(row)

        file.close()

    def do_merge_routes(self, reader: csv.DictReader[str], writer: csv.DictWriter[str],
                        agency_id: str, prefix: str, route_names: Dict[str, str] = {}) -> None:
        for row in reader:
            row["agency_id"] = agency_id
            row["route_id"] = prefix + row["route_id"]

            row["route_short_name"] = row["route_short_name"].strip()

            row["route_long_name"] = route_names.get(row["route_short_name"], "") \
                .replace('""', '"')
            row["route_color"], row["route_text_color"] = route_color(row["agency_id"],
                                                                      row["route_type"])

            writer.writerow(row)

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
        with csv_reader_from_zip(self.gdansk, "routes.txt") as reader:
            self.do_merge_routes(reader, writer, "1", "1:", route_names)

        print("\033[1A\033[K" + "Merging Gdynia routes")
        with csv_reader_from_zip(self.gdynia, "routes.txt") as reader:
            self.do_merge_routes(reader, writer, "2", "2:", route_names)

    def load_dates(self, reader: csv.DictReader[str], prefix: str) -> List[ServiceDate]:
        return [
            ServiceDate(
                prefix + row["service_id"],
                datetime.strptime(row["date"], "%Y%m%d").date(),
            )
            for row in reader
            if row["excpetion_type"] == "1"
        ]

    def merge_dates(self) -> None:
        file = open("gtfs/calendar_dates.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(file, ["date", "service_id", "exception_type"],
                                extrasaction="ignore")
        writer.writeheader()

        print("\033[1A\033[K" + "Loading Gdańsk services")
        with csv_reader_from_zip(self.gdansk, "calendar_dates.txt") as reader:
            gdansk_dates = self.load_dates(reader, "1:")

        print("\033[1A\033[K" + "Loading Gdynia services")
        with csv_reader_from_zip(self.gdynia, "calendar_dates.txt") as reader:
            gdynia_dates = self.load_dates(reader, "2:")

        print("\033[1A\033[K" + "Exporting all services")

        # Find common start & end date for calendars
        start_date: date = max(
            min(i.date for i in gdansk_dates),
            min(i.date for i in gdynia_dates),
        )

        end_date: date = min(
            max(i.date for i in gdansk_dates),
            max(i.date for i in gdynia_dates),
        )

        # Figure out which service is active on every day between start_date and end_date
        services_on_date: Dict[date, Set[str]] = {}
        for service_date in chain(gdansk_dates, gdynia_dates):
            if service_date.date > end_date or service_date.date < start_date:
                continue

            services_on_date.setdefault(service_date.date, set()) \
                            .add(service_date.service_id)

        # Save merged services
        while start_date <= end_date:
            date_str = start_date.strftime("%Y%m%d")

            for service in sorted(services_on_date[start_date]):
                writer.writerow(
                    {"date": date_str, "service_id": service, "exception_type": "1"}
                )
                self.active_services.add(service)

            start_date += timedelta(days=1)

        file.close()

    def do_merge_times(self, writer: csv.DictWriter[str], reader: csv.DictReader[str],
                       prefix: str) -> None:
        for row in reader:
            row["trip_id"] = prefix + row["trip_id"]
            # row["stop_id"] = self.stop_merge_table.get(row["stop_id"], row["stop_id"])

            if row["trip_id"] not in self.active_trips:
                continue

            writer.writerow(row)

    def merge_times(self) -> None:
        file = open("gtfs/stop_times.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(
            file,
            [
                "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
                "pickup_type", "drop_off_type",
            ],
            extrasaction="ignore",
        )
        writer.writeheader()

        print("\033[1A\033[K" + "Merging Gdańsk stop_times")
        with csv_reader_from_zip(self.gdansk, "stop_times.txt") as reader:
            self.do_merge_times(writer, reader, "1:")

        print("\033[1A\033[K" + "Merging Gdynia stop_times")
        with csv_reader_from_zip(self.gdynia, "stop_times.txt") as reader:
            self.do_merge_times(writer, reader, "2:")

        file.close()

    def do_merge_shapes(self, writer: csv.DictWriter[str], reader: csv.DictReader[str],
                        prefix: str) -> None:
        for row in reader:
            row["shape_id"] = prefix + row["shape_id"]
            if row["shape_id"] not in self.active_shapes:
                continue

            writer.writerow(row)

    def merge_shapes(self) -> None:
        file = open("gtfs/shapes.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(
            file,
            ["shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"],
            extrasaction="ignore")
        writer.writeheader()

        print("\033[1A\033[K" + "Merging Gdańsk shapes")
        with csv_reader_from_zip(self.gdansk, "shapes.txt") as reader:
            self.do_merge_shapes(writer, reader, "1:")

        print("\033[1A\033[K" + "Merging Gdynia shapes")
        with csv_reader_from_zip(self.gdansk, "shapes.txt") as reader:
            self.do_merge_shapes(writer, reader, "2:")

        file.close()

    def do_merge_trips(self, writer: csv.DictWriter[str], reader: csv.DictReader[str],
                       prefix: str) -> None:
        for row in reader:
            row["route_id"] = prefix + row["route_id"]
            row["service_id"] = prefix + row["service_id"]
            row["trip_id"] = prefix + row["trip_id"]
            row["shape_id"] = prefix + row["shape_id"]

            if row["service_id"] not in self.active_services:
                continue

            row["trip_headsign"] = row["trip_headsign"].rstrip(" 0123456789")

            self.active_trips.add(row["trip_id"])
            self.active_shapes.add(row["shape_id"])
            writer.writerow(row)

    def merge_trips(self) -> None:
        file = open("gtfs/trips.txt", mode="w", encoding="utf-8", newline="")
        writer = csv.DictWriter(
            file,
            ["route_id", "service_id", "trip_id", "trip_headsign",
             "direction_id", "shape_id", "wheelchair_accessible"],
            extrasaction="ignore")
        writer.writeheader()

        print("\033[1A\033[K" + "Merging Gdańsk trips")
        with csv_reader_from_zip(self.gdansk, "trips.txt") as reader:
            self.do_merge_trips(writer, reader, "1:")

        print("\033[1A\033[K" + "Merging Gdynia trips")
        with csv_reader_from_zip(self.gdynia, "trips.txt") as reader:
            self.do_merge_trips(writer, reader, "2:")

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
    argprs.add_argument("-o", "--output-file", default="gtfs.zip", required=False,
                        metavar="(path)", dest="target",
                        help="destination of the gtfs file (defualt: gtfs.zip)")
    argprs.add_argument("-pn", "--publisher-name", required=False, metavar="NAME",
                        dest="publisher_name", help="value of feed_publisher_name")
    argprs.add_argument("-pu", "--publisher-url", required=False, metavar="URL",
                        dest="publisher_url", help="value of feed_publisher_url")

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
