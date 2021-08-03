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


    def fetchRoutesByLatLong(self, latitude: float, longitude: str, maximumDistance: float, distanceUnits: str = "mi") -> list:
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
                select  {earthRadius} * 2 * asin(sqrt(sin((radians(a.Latitude) - radians({latitude})) / 2) ^ 2 
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

    def fetchRoutesByCityState(self, city: str, state: str, maximumDistance: float, distanceUnits: str = "mi") -> list:
        if distanceUnits.lower() not in {"mi", "miles", "km", "kilometers"}:
            raise ValueError("distanceUnits must be on of the following: ['mi', 'miles', 'km', 'kilometers'].")

        cityStateLocation = self.geoAgent.geocode(query=", ".join([city.strip(), state.strip()]), exactly_one=True)

        if cityStateLocation is None:
            raise ValueError(f"Could not locate coordinates for city state combination {', '.join([city.strip(), state.strip()])}")

        latitude = cityStateLocation.raw["lat"]
        longitude = cityStateLocation.raw["lon"]

        return self.fetchRoutesByLatLong(
            latitude=latitude,
            longitude=longitude,
            maximumDistance=maximumDistance,
            distanceUnits=distanceUnits
        )

    def fetchRouteByURL(self, routeURL: str) -> tuple:
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

    def fetchRoutesByArea(self, areaName: str) -> list:
        query = f"""
        ; with recursive SubAreas as (
            select AreaId
                from Areas
                where AreaName = '{areaName}'
            union all
            select a.AreaId
                from Areas a
                inner join SubAreas s
                    on s.AreaId = a.ParentAreaId
        )
        select *
            from Routes r
            inner join SubAreas s
                on s.AreaId = r.AreaId
        """

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def processFilters(self, **kwargs: dict) -> str:
        if not kwargs:
            return "", ""
        else:
            distanceJoinClause = ""
            whereClause = "where true "

        kwargs = {key.lower(): value for key, value in kwargs.items()}

        keys = kwargs.keys()

        if "type" in keys:
            type = kwargs["type"].lower()
            if "trad" in type:
                whereClause += f"and (lower(r.Type) like '%trad%' and lower(r.Type) not like '%aid%' and lower(r.Type) not like '%mixed%' and lower(r.Type) not like '%ice%' and lower(r.Type) not like '%snow%') "

            if "aid" in type:
                whereClause += f"and (lower(r.Type) like '%aid%') "

            if "sport" in type:
                whereClause += f"and (lower(r.Type) like '%sport%' and lower(r.Type) not like '{'%trad%' if 'trad' not in type else ''}' and lower(r.Type) not like '%aid%' and lower(r.Type) not like '%mixed%' and lower(r.Type) not like '%ice%' and lower(r.Type) not like '%snow%') "

            if "boulder" in type:
                whereClause += f"and (lower(r.Type) like '%boulder%' and lower(r.Type) not like '%trad%') "

            if "top rope" in type:
                whereClause += f"and (lower(r.Type) like '%toprope%') "

            if "alpine" in type:
                whereClause += f"and (lower(r.Type) like '%alpine%') "

            if "ice" in type:
                whereClause += f"and (lower(r.Type) like '%ice%') "

            if "snow" in type:
                whereClause += f"and (lower(r.Type) like '%snow%') "

            if "mixed" in type:
                whereClause += f"and (lower(r.Type) like '%mixed%') "

        if "height" in keys:
            height = str(kwargs["height"]).lower().strip()
            if height.endswith("-"):
                greaterThan = False
            else:
                greaterThan = True

            height = height.strip("-").strip("+")

            try:
                float(height)
            except ValueError as e:
                raise ValueError(f"Height is not a valid number.")

            if greaterThan:
                whereClause += f"and (r.Height >= {height}) "
            else:
                whereClause += f"and (r.Height <= {height}) "

        if "pitches" in keys:
            pitches = str(kwargs["pitches"]).lower().strip()
            if pitches.endswith("-"):
                greaterThan = False
            elif pitches.endswith("="):
                greaterThan = None
            else:
                greaterThan = True

            pitches = pitches.strip("-").strip("+").strip("=")

            try:
                float(pitches)
            except ValueError as e:
                raise ValueError(f"Pitches is not a valid number.")

            if greaterThan:
                whereClause += f"and (r.Pitches >= {pitches}) "
            elif greaterThan is None:
                whereClause += f"and (r.Pitches = {pitches}) "
            else:
                whereClause += f"and (r.Pitches <= {pitches}) "

        if "grade" in keys:
            grade = str(kwargs["grade"]).lower().strip()
            if grade.endswith("-"):
                greaterThan = False
            elif grade.endswith("="):
                greaterThan = None
            else:
                greaterThan = True

            grade = grade.strip("-").strip("+").strip("=")

            try:
                float(grade)
            except ValueError as e:
                raise ValueError(f"Grade is not a valid number.")

            if greaterThan:
                whereClause += f"and (r.Grade >= {grade}) "
            elif greaterThan is None:
                whereClause += f"and (r.Grade = {grade}) "
            else:
                whereClause += f"and (r.Grade <= {grade}) "

        if "averagerating" in keys:
            averageRating = str(kwargs["averagerating"]).lower().strip()
            if averageRating.endswith("-"):
                greaterThan = False
            else:
                greaterThan = True

            averageRating = averageRating.strip("-").strip("+")

            try:
                float(averageRating)
            except ValueError as e:
                raise ValueError(f"Average rating is not a valid number.")

            if greaterThan:
                whereClause += f"and (r.AverageRating >= {averageRating}) "
            else:
                whereClause += f"and (r.AverageRating <= {averageRating}) "

        if "votecount" in keys:
            voteCount = str(kwargs["votecount"]).lower().strip()
            if voteCount.endswith("-"):
                greaterThan = False
            else:
                greaterThan = True

            voteCount = voteCount.strip("-").strip("+")

            try:
                float(voteCount)
            except ValueError as e:
                raise ValueError(f"Vote count is not a valid number.")

            if greaterThan:
                whereClause += f"and (r.Votecount >= {voteCount}) "
            else:
                whereClause += f"and (r.VoteCount <= {voteCount}) "

        if any(keyword in keys for keyword in {"city", "state", "latitude", "longitude", "radius", "proximityroute"}):
            latitude, longitude = None, None

            if "city" in keys or "state" in keys:
                if not all(keyword in keys for keyword in {"city", "state", "radius"}):
                    raise ValueError("Error: All three of city, state, and radius must be specified.")

                city, state = kwargs["city"], kwargs["state"]
                cityStateLocation = self.geoAgent.geocode(
                    query=", ".join([city.strip(), state.strip()]),
                    exactly_one=True
                )

                if cityStateLocation is None:
                    raise ValueError(
                        f"Could not locate coordinates for city state combination {', '.join([city.strip(), state.strip()])}")

                latitude = cityStateLocation.raw["lat"]
                longitude = cityStateLocation.raw["lon"]

            if "latitude" in keys or "longitude" in keys:
                if not all(keyword in keys for keyword in {"latitude", "longitude", "radius"}):
                    raise ValueError("Error: All three of latitude, longitude, and radius must be specified.")
                latitude = kwargs["latitude"]
                longitude = kwargs["longitude"]

            if "proximityroute" in keys:
                if not all(keyword in keys for keyword in {"proximityroute", "radius"}):
                    raise ValueError("Error: Both a proximity route URL and radius must be specified.")

                routeId = re.search(pattern=r"\d+", string=str(kwargs["proximityroute"]))

                if routeId is not None:
                    routeId = int(routeId.group(0))
                else:
                    raise ValueError(f"Error: Cannot parse RouteId from specified URL ({routeURL}).")

                query = f"""
                    select  a.Latitude,
                            a.Longitude
                        from Routes r
                        inner join Areas a
                            on a.AreaId = r.AreaId
                        where r.RouteId = {routeId}
                        """

                self.cursor.execute(query)
                latitude, longitude = self.cursor.fetchone()

            if latitude is None or longitude is None:
                raise ValueError("You must specify a way to obtain latitude/longitude. "
                                 "Options are city/state, latitude/longitude, or a proximity route URL.")

            radius = str(kwargs["radius"]).lower().strip()

            if radius.endswith("-"):
                greaterThan = False
            else:
                greaterThan = True

            radius = radius.strip("-").strip("+")

            distanceUnits = kwargs["distanceunits"].lower() if "distanceunits" in kwargs.keys() else "miles"
            if distanceUnits not in {"mi", "miles", "km", "kilometers"}:
                raise ValueError("distanceUnits must be one of the following: ['mi', 'miles', 'km', 'kilometers'].")

            if distanceUnits in {"km", "kilometers"}:
                earthRadius = 6371.0
            else:
                earthRadius = 3958.8

            distanceJoinClause += f"""
                inner join Areas a
                    on a.AreaId = r.AreaId
                left join lateral (
                    select  {earthRadius} * 2 * asin(sqrt(sin((radians(a.Latitude) - radians({latitude})) / 2) ^ 2 
                            + cos(radians({latitude})) * cos(radians(a.Latitude)) * sin((radians(a.Longitude) - radians({longitude})) / 2) ^ 2)) as Distance
                ) d
                    on true
            """

            try:
                float(radius)
            except ValueError as e:
                raise ValueError(f"Radius count is not a valid number.")

            whereClause += f"and (a.Latitude is not null) "
            whereClause += f"and (a.Longitude is not null) "
            whereClause += f"and (a.AreaId != 112166257) " # Filter out generic area

            if greaterThan:
                whereClause += f"and (d.Distance >= {radius}) "
            else:
                whereClause += f"and (d.Distance <= {radius}) "

        return distanceJoinClause, whereClause

    def fetchRoutes(self, **kwargs) -> list:
        distanceJoinClause, whereClause = self.processFilters(**kwargs)

        parentAreaName = kwargs["parentAreaName"] if "parentAreaName" in kwargs.keys() else None

        if parentAreaName is None:
            query = f"""
            select r.*
                from Routes r
                {distanceJoinClause}
                {whereClause};
            """
        else:
            query = f"""
            ; with recursive SubAreas as (
                select AreaId
                    from Areas
                    where AreaName like '%{parentAreaName}%'
                union all
                select a.AreaId
                    from Areas a
                    inner join SubAreas s
                        on s.AreaId = a.ParentAreaId
            )
            select r.*
                from Routes r
                inner join SubAreas s
                    on s.AreaId = r.AreaId
                {distanceJoinClause}
                {whereClause};
            """

        self.cursor.execute(query)

        return self.cursor.fetchall()


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

    routes = pipe.fetchRoutes(radius="0")

    print(len(routes))