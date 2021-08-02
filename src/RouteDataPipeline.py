import os
import psycopg2
import re

from geopy import geocoders
from dotenv import load_dotenv


class RoutePipeline(object):
    def __init__(self, username: str, password: str, host: str, port: str, database: str, geopyUsername: str):
        self.connection = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database
        )
        self.cursor = self.connection.cursor()

        self.geoAgent = geocoders.Nominatim(user_agent="zsnyder21")

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def fetchRoutesByCityState(self, city: str, state: str, maximumDistance: float, distanceUnits: str = "mi"):
        if distanceUnits.lower() not in {"mi", "miles", "km", "kilometers"}:
            raise ValueError("distanceUnits must be on of the following: ['mi', 'miles', 'km', 'kilometers'].")

        if distanceUnits.lower() in {"km", "kilometers"}:
            earthRadius = 6371.0
        else:
            earthRadius = 3958.8

        cityStateLocation = self.geoAgent.geocode(query=", ".join([city.strip(), state.strip()]), exactly_one=True)
        latitude = cityStateLocation.raw["lat"]
        longitude = cityStateLocation.raw["lon"]

        query = f"""
        select r.*
            from Routes r
            inner join Areas a
                on a.AreaId = r.AreaId
            left join lateral (
                select {earthRadius} * 2 * asin(sqrt(sin((radians(a.Latitude) - radians({latitude})) / 2) ^ 2 
			+ cos(radians({latitude})) * cos(radians(a.Latitude)) * sin((radians(a.Longitude) - radians({longitude})) / 2) ^ 2)) as Distance
            ) d
                on true
            where a.Latitude is not null
                and a.Longitude is not null
                and a.AreaId != 112166257  -- Filter out generic area
                and d.Distance <= {maximumDistance}
        """

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def fetchRoutesByLatLong(self, latitude: float, longitude: str, maximumDistance: float, distanceUnits: str = "mi"):
        if distanceUnits.lower() not in {"mi", "miles", "km", "kilometers"}:
            raise ValueError("distanceUnits must be on of the following: ['mi', 'miles', 'km', 'kilometers'].")

        if distanceUnits.lower() in {"km", "kilometers"}:
            earthRadius = 6371.0
        else:
            earthRadius = 3958.8

        query = f"""
        select r.*
            from Routes r
            inner join Areas a
                on a.AreaId = r.AreaId
            left join lateral (
                select {earthRadius} * 2 * asin(sqrt(sin((radians(a.Latitude) - radians({latitude})) / 2) ^ 2 
            + cos(radians({latitude})) * cos(radians(a.Latitude)) * sin((radians(a.Longitude) - radians({longitude})) / 2) ^ 2)) as Distance
            ) d
                on true
            where a.Latitude is not null
                and a.Longitude is not null
                and a.AreaId != 112166257  -- Filter out generic area
                and d.Distance <= {maximumDistance}
        """

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def fetchRouteByURL(self, routeURL: str):
        routeId = re.search(pattern=r"\d+", string=routeURL)

        if routeId is not None:
            routeId = int(routeId.group(0))
        else:
            raise ValueError(f"Error: Cannot parse RouteId from specified URL ({routeURL}).")

        query = f"""
        select  a.Latitude,
                a.Longitude,
                r.*
            from Routes r
            inner join Areas a
                on a.AreaId = r.AreaId
            where r.RouteId = {routeId}
        """

        self.cursor.execute(query)
        return self.cursor.fetchone()


if __name__ == "__main__":
    load_dotenv()

    pipe = RoutePipeline(
        username="postgres",
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host="127.0.0.1",
        port="5432",
        database="MountainProject",
        geopyUsername="zsnyder21"
    )

    # routes = pipe.getRoutesByCityState(
    #     city="Boulder",
    #     state="CO",
    #     maximumDistance=25,
    #     distanceUnits="mi"
    # )

    route = pipe.fetchRouteByURL(routeURL="https://www.mountainproject.com/route/105750457/cosmosis")
    print(route[0], route[1])

    test = pipe.fetchRoutesByLatLong(latitude=route[0], longitude=route[1], maximumDistance=50, distanceUnits="miles")
    print(test)