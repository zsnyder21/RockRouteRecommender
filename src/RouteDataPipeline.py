import os
import psycopg2
import re

from geopy import geocoders
from dotenv import load_dotenv


class RoutePipeline(object):
    """
    This class serves as a concise object allowing us to filter routes
    from the MountainProject database in complex and useful ways
    """
    def __init__(self, username: str, password: str, host: str, port: str, database: str, geopyUsername: str) -> None:
        """

        :param username: PostgreSQL username
        :param password: PostgreSQL password
        :param host: PostgreSQL host
        :param port: PostgreSQL port
        :param database: PostgreSQL database
        :param geopyUsername: geopy user_agent
        """
        self.connection = psycopg2.connect(
            user=username,
            password=password,
            host=host,
            port=port,
            database=database
        )
        self.cursor = self.connection.cursor()

        self.geoAgent = geocoders.Nominatim(user_agent="zsnyder21")

    def __del__(self) -> None:
        """
        Clean up the PostgreSQL connection

        :return: None
        """
        self.cursor.close()
        self.connection.close()

    def fetchRatingSystemDifficulties(self) -> dict:
        """
        Collects and returns all possible difficulties for all rating
        systems present in the database
        :return: Dictionary of lists containing difficulties for each rating system
        """
        difficultySystemValues = dict()

        query = """
        select	distinct RatingSystem,
                case when RatingSystem = 'YDS' then 1
                          when RatingSystem = 'V' then 2
                          when RatingSystem = 'C' then 3
                          when RatingSystem = 'A' then 4
                          when RatingSystem = 'Snow' then 5
                          when RatingSystem = 'AI' then 6
                          when RatingSystem = 'WI' then 7
                          when RatingSystem = 'M' then 8 end as ordering,

                case when RatingSystem = 'YDS' then 'Yosemite Decimal'
                          when RatingSystem = 'V' then 'V Scale'
                          when RatingSystem = 'C' then 'Clean Aid'
                          when RatingSystem = 'A' then 'Aid'
                          when RatingSystem = 'Snow' then 'Snow'
                          when RatingSystem = 'AI' then 'Alpine Ice'
                          when RatingSystem = 'WI' then 'Winter Ice'
                          when RatingSystem = 'M' then 'Mixed' end as ratingsystemname
            from difficultyreference
            order by 2
        """

        self.cursor.execute(query)
        difficultySystems = self.cursor.fetchall()

        for ratingSystem, _, ratingSystemName in difficultySystems:
            query = f"""
            select Difficulty, DifficultyRanking
                from DifficultyReference
                where RatingSystem = '{ratingSystem}'
                    and Difficulty != '5th'
                order by 2
            """

            self.cursor.execute(query)
            ratingSystemRatings = [item[0] for item in self.cursor.fetchall()]

            difficultySystemValues[ratingSystemName] = ratingSystemRatings

        return difficultySystemValues

    def fetchRoutesByLatLong(self, latitude: float, longitude: str, maximumDistance: float, distanceUnits: str = "mi") -> list:
        """
        Fetch routes within a specified distance of a supplied latitude and longitude

        :param latitude: Latitude
        :param longitude: Longitude
        :param maximumDistance: Maximum distance
        :param distanceUnits: Units to user (mi, miles, km, kilometers)
        :return: List of routes within the specified distance of the specified latitude/longitude
        """
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
        """
        Fetch routes within a specified distance of a specified city and state

        :param city: City
        :param state: State
        :param maximumDistance: Maximum distance
        :param distanceUnits: Units to user (mi, miles, km, kilometers)
        :return: List of routes within the specified distance of the specified city/state
        """
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
        """
        Fetch route information by specified URL

        :param routeURL: URL of the route to fetch
        :return: Route information of the specified route
        """
        routeId = re.search(pattern=r"\d+", string=str(routeURL))

        if routeId is not None:
            routeId = int(routeId.group(0))
        else:
            raise ValueError(f"Error: Cannot parse RouteId from specified URL ({routeURL}).")

        query = f"""
        select  r.RouteId,
                r.AreaId,
                r.RouteName,
                r.Difficulty_YDS,
                r.Difficulty_ADL,
                r.Severity,
                r.Type,
                r.Height,
                r.Pitches,
                r.Grade,
                coalesce(r.Description, '') as Description ,
                r.Location,
                coalesce(r.Protection, '') as Protection,
                r.FirstAscent,
                r.FirstAscentYear,
                r.FirstFreeAscent,
                r.FirstFreeAscentYear,
                r.AverageRating,
                r.VoteCount,
                r.URL,
                coalesce(string_agg(c.CommentBody, chr(10)||chr(13)||chr(10)||chr(13)), '') as Comments
            from Routes r
            left join RouteComments c
                on c.RouteId = r.RouteId
            where r.RouteId = {routeId}
            group by r.RouteId,
                r.AreaId,
                r.RouteName,
                r.Difficulty_YDS,
                r.Difficulty_ADL,
                r.Severity,
                r.Type,
                r.Height,
                r.Pitches,
                r.Grade,
                coalesce(r.Description, ''),
                r.Location,
                coalesce(r.Protection, ''),
                r.FirstAscent,
                r.FirstAscentYear,
                r.FirstFreeAscent,
                r.FirstFreeAscentYear,
                r.AverageRating,
                r.VoteCount,
                r.URL;
        """

        self.cursor.execute(query)
        results = self.cursor.fetchone()

        if not results:
            raise ValueError(f"Could not locate a route matching the URL specified: {routeURL}.")

        fields = [
            "RouteId",
            "AreaId",
            "RouteName",
            "Difficulty_YDS",
            "Difficulty_ADL",
            "Severity",
            "Type",
            "Height",
            "Pitches",
            "Grade",
            "Description",
            "Location",
            "Protection",
            "FirstAscent",
            "FirstAscentYear",
            "FirstFreeAscent",
            "FirstFreeAscentYear",
            "AverageRating",
            "VoteCount",
            "URL",
            "Comments"
        ]
        fieldCount = len(fields)

        return {fields[idx]: results[idx] for idx in range(fieldCount)}

    def fetchRouteRatingsByURL(self, routeURL: str) -> tuple:
        """
        Fetch route information by specified URL

        :param routeURL: URL of the route to fetch
        :return: Route information of the specified route
        """
        routeId = re.search(pattern=r"\d+", string=str(routeURL))

        if routeId is not None:
            routeId = int(routeId.group(0))
        else:
            raise ValueError(f"Error: Cannot parse RouteId from specified URL ({routeURL}).")

        queryParameters = {"routeId": routeId}
        query = f"""
        select  r.RouteId,
                rr.UserId,
                rr.Rating
            from Routes r
            inner join RouteRatings rr
                on rr.RouteId = r.RouteID
            where r.RouteId = %(routeId)s
                and rr.UserId is not null;
        """

        self.cursor.execute(query, queryParameters)
        results = self.cursor.fetchall()

        if not results:
            raise ValueError(f"Could not locate a route matching the URL specified: {routeURL}.")

        fields = [
            "RouteId",
            "UserId",
            "Rating"
        ]
        fieldCount = len(fields)

        return [{fields[idx]: result[idx] for idx in range(fieldCount)} for result in results]

    def fetchRoutesByArea(self, areaName: str) -> list:
        """
        Fetch all routes that live underneath a given area

        :param areaName: Name of the area to find routes under
        :return: List of routes that live under the specified area
        """
        queryParameters = {"areaName": areaName}
        query = f"""
        ; with recursive SubAreas as (
            select AreaId
                from Areas
                where AreaName = %(areaName)s
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

        self.cursor.execute(query, queryParameters)
        return self.cursor.fetchall()

    def processFilters(self, **kwargs) -> tuple:
        """
        This method builds the necessary join and where clauses to gather
        all information as specified by the user in keyword arguments. Note
        that any parameter can be omitted to avoid filtering on it. Some
        parameters are required combinations, such as city/state, or
        latitude/longitude.

        :param kwargs: Keyword arguments to create filters with
        :return: Any necessary join clauses and where clauses to add into the SQL query as strings.
        """
        if not kwargs:
            return "", "where true", None
        else:
            joinClause = ""
            whereClause = "where true "
            queryParameters = {}

        kwargs = {key.lower(): value for key, value in kwargs.items()}

        keys = kwargs.keys()

        if any(keyword in keys for keyword in {"routedifficultylow", "routedifficultyhigh"}):
            routeDifficultyLow = kwargs["routedifficultylow"] if "routedifficultylow" in keys else None
            routeDifficultyHigh = kwargs["routedifficultyhigh"] if "routedifficultyhigh" in keys else None
            routeDifficultyPattern = (routeDifficultyLow or routeDifficultyHigh)
            routeDifficultyPatterns = self.fetchRatingSystemDifficulties()

            # Determine what types of routes we are looking for difficulty on
            if routeDifficultyPattern in routeDifficultyPatterns["Yosemite Decimal"]:
                ratingSystem = "YDS"
            elif routeDifficultyPattern in routeDifficultyPatterns["V Scale"]:
                ratingSystem = "V"
            elif routeDifficultyPattern in routeDifficultyPatterns["Winter Ice"]:
                ratingSystem = "WI"
            elif routeDifficultyPattern in routeDifficultyPatterns["Alpine Ice"]:
                ratingSystem = "AI"
            elif routeDifficultyPattern in routeDifficultyPatterns["Mixed"]:
                ratingSystem = "M"
            elif routeDifficultyPattern in routeDifficultyPatterns["Snow"]:
                ratingSystem = "Snow"
            elif routeDifficultyPattern in routeDifficultyPatterns["Aid"]:
                ratingSystem = "A"
            elif routeDifficultyPattern in routeDifficultyPatterns["Clean Aid"]:
                ratingSystem = "C"
            else:
                raise ValueError(f"Could not determine what difficulty metric to use based on input "
                                 f"{routeDifficultyPattern}.")

            # Fetch numeric values corresponding to the route difficulties passed in
            if routeDifficultyLow:
                query = f"""
                    select DifficultyRanking
                        from DifficultyReference
                        where Difficulty = %(routeDifficultyLow)s
                            and RatingSystem = %(ratingSystem)s
                """
            else:
                query = f"""
                    select min(DifficultyRanking)
                        from DifficultyReference
                        where RatingSystem = %(ratingSystem)s
                """

            self.cursor.execute(query, {"routeDifficultyLow": routeDifficultyLow, "ratingSystem": ratingSystem})
            difficultyLow = self.cursor.fetchone()[0]

            if routeDifficultyHigh:
                query = f"""
                    select DifficultyRanking
                        from DifficultyReference
                        where Difficulty = %(routeDifficultyHigh)s
                            and RatingSystem = %(ratingSystem)s
                """
            else:
                query = f"""
                    select max(DifficultyRanking)
                        from DifficultyReference
                        where RatingSystem = %(ratingSystem)s
                """

            self.cursor.execute(query, {"routeDifficultyHigh": routeDifficultyHigh, "ratingSystem": ratingSystem})
            difficultyHigh = self.cursor.fetchone()[0]

            queryParameters["difficultyHigh"] = difficultyHigh
            queryParameters["difficultyLow"] = difficultyLow
            queryParameters["ratingSystem"] = ratingSystem
            joinClause += f"""
                left join lateral (
                    select unnest(string_to_array(coalesce(r.Difficulty_ADL, ''), ' ')) as Difficulty
                    union all
                    select r.Difficulty_YDS
                ) l0
                    on true
                left join lateral (
                    select case when l0.Difficulty = 'Steep' then 'Steep Snow'
                                when l0.Difficulty = 'Mod.' then 'Mod. Snow'
                                when l0.Difficulty = 'Easy' then 'Easy Snow'
                                else l0.Difficulty end as Difficulty
                ) l
                    on true
                inner join DifficultyReference ref
                    on l.Difficulty = ref.Difficulty
            """

            whereClause += f"and (ref.DifficultyRanking <= %(difficultyHigh)s) "
            whereClause += f"and (ref.DifficultyRanking >= %(difficultyLow)s) "
            whereClause += f"and (ref.RatingSystem = %(ratingSystem)s) "

        if "type" in keys:
            type = kwargs["type"].lower()
            typeWhereClause = "and (false "

            if "trad" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%trad%%' and lower(r.Type) not like '%%aid%%' and lower(r.Type) not like '%%mixed%%' and lower(r.Type) not like '%%ice%%' and lower(r.Type) not like '%%snow%%') "

            if "aid" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%aid%%') "

            if "sport" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%sport%%' and lower(r.Type) not like '%%trad%%' and lower(r.Type) not like '%%aid%%' and lower(r.Type) not like '%%mixed%%' and lower(r.Type) not like '%%ice%%' and lower(r.Type) not like '%%snow%%') "

            if "boulder" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%boulder%%' and lower(r.Type) not like '%%trad%%') "

            if "top rope" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%top rope%%') "

            if "alpine" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%alpine%%') "

            if "ice" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%ice%%') "

            if "snow" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%snow%%') "

            if "mixed" in type:
                typeWhereClause += f"or (lower(r.Type) like '%%mixed%%') "

            if typeWhereClause == "and (false ":
                raise ValueError("Invalid route type specified. Valid types are Sport, Trad, Aid, Boulder, Top Rope, Alpine, Ice, Snow, and Mixed")
            else:
                typeWhereClause += f") "

            whereClause += typeWhereClause

        if "severitythreshold" in keys:
            severityThreshold = kwargs["severitythreshold"].upper()
            query = f"""
                select SeverityRanking
                    from SeverityReference
                    where Severity = %(severityThreshold)s
            """

            self.cursor.execute(query, {"severityThreshold": severityThreshold})

            severity = self.cursor.fetchone()[0]

            joinClause += f"""
                inner join SeverityReference sev
                    on sev.Severity = coalesce(r.Severity, 'G')
            """
            queryParameters["severity"] = severity
            whereClause += f"and (sev.SeverityRanking <= %(severity)s) "

        if "height" in keys:
            height = str(kwargs["height"]).lower().strip()
            if height.endswith("-"):
                greaterThan = False
            else:
                greaterThan = True

            height = height.strip("-").strip("+")

            try:
                height = float(height)
            except ValueError as e:
                raise ValueError(f"Height is not a valid number.")

            queryParameters["height"] = height
            if greaterThan:
                whereClause += f"and (r.Height >= %(height)s) "
            else:
                whereClause += f"and (r.Height <= %(height)s) "

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
                pitches = float(pitches)
            except ValueError as e:
                raise ValueError(f"Pitches is not a valid number.")

            queryParameters["pitches"] = pitches
            if greaterThan:
                whereClause += f"and (r.Pitches >= %(pitches)s) "
            elif greaterThan is None:
                whereClause += f"and (r.Pitches = %(pitches)s) "
            else:
                whereClause += f"and (r.Pitches <= %(pitches)s) "

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
                grade = float(grade)
            except ValueError as e:
                raise ValueError(f"Grade is not a valid number.")

            queryParameters["grade"] = grade
            if greaterThan:
                whereClause += f"and (r.Grade >= %(grade)s) "
            elif greaterThan is None:
                whereClause += f"and (r.Grade = %(grade)s) "
            else:
                whereClause += f"and (r.Grade <= %(grade)s) "

        if "averagerating" in keys:
            averageRating = str(kwargs["averagerating"]).lower().strip()
            if averageRating.endswith("-"):
                greaterThan = False
            else:
                greaterThan = True

            averageRating = averageRating.strip("-").strip("+")

            try:
                averageRating = float(averageRating)
            except ValueError as e:
                raise ValueError(f"Average rating is not a valid number.")

            queryParameters["averageRating"] = averageRating
            if greaterThan:
                whereClause += f"and (r.AverageRating >= %(averageRating)s) "
            else:
                whereClause += f"and (r.AverageRating <= %(averageRating)s) "

        if "votecount" in keys:
            voteCount = str(kwargs["votecount"]).lower().strip()
            if voteCount.endswith("-"):
                greaterThan = False
            else:
                greaterThan = True

            voteCount = voteCount.strip("-").strip("+")

            try:
                voteCount = float(voteCount)
            except ValueError as e:
                raise ValueError(f"Vote count is not a valid number.")

            queryParameters["voteCount"] = voteCount
            if greaterThan:
                whereClause += f"and (r.Votecount >= %(voteCount)s) "
            else:
                whereClause += f"and (r.VoteCount <= %(voteCount)s) "

        if "elevation" in keys:
            elevation = str(kwargs["elevation"]).lower().strip()
            if elevation.endswith("-"):
                greaterThan = False
            else:
                greaterThan = True

            elevation = elevation.strip("-").strip("+")

            try:
                elevation = float(elevation)
            except ValueError as e:
                raise ValueError(f"Elevation is not a valid number.")

            joinClause += f"""
            inner join Areas e
                on e.AreaId = r.AreaId
            """

            queryParameters["elevation"] = elevation
            if greaterThan:
                whereClause += f"and (e.Elevation >= %(elevation)s) "
            else:
                whereClause += f"and (e.Elevation <= %(elevation)s) "

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

                latitude = float(cityStateLocation.raw["lat"])
                longitude = float(cityStateLocation.raw["lon"])

            if latitude is None and ("latitude" in keys or "longitude" in keys):
                if not all(keyword in keys for keyword in {"latitude", "longitude", "radius"}):
                    raise ValueError("Error: All three of latitude, longitude, and radius must be specified.")
                latitude = float(kwargs["latitude"])
                longitude = float(kwargs["longitude"])

            if latitude is None and "proximityroute" in keys:
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
                        where r.RouteId = %(routeId)s
                            or a.AreaId = %(routeId)s;
                        """

                self.cursor.execute(query, {"routeId": routeId})
                latitude, longitude = map(float, self.cursor.fetchone())

            if latitude is None or longitude is None:
                raise ValueError("You must specify a way to obtain latitude/longitude. "
                                 "Options are city/state, latitude/longitude, or a proximity route URL.")

            radius = str(kwargs["radius"]).lower().strip()

            if radius.endswith("+"):
                greaterThan = True
            else:
                greaterThan = False

            radius = radius.strip("-").strip("+")

            distanceUnits = kwargs["distanceunits"].lower() if "distanceunits" in kwargs.keys() else "miles"
            if distanceUnits not in {"mi", "miles", "km", "kilometers"}:
                raise ValueError("distanceUnits must be one of the following: ['mi', 'miles', 'km', 'kilometers'].")

            if distanceUnits in {"km", "kilometers"}:
                earthRadius = 6371.0
            else:
                earthRadius = 3958.8

            queryParameters["earthRadius"] = earthRadius
            queryParameters["latitude"] = latitude
            queryParameters["longitude"] = longitude
            joinClause += f"""
                inner join Areas a
                    on a.AreaId = r.AreaId
                left join lateral (
                    select  %(earthRadius)s * 2 * asin(sqrt(sin((radians(a.Latitude) - radians(%(latitude)s)) / 2) ^ 2 
                            + cos(radians(%(latitude)s)) * cos(radians(a.Latitude)) * sin((radians(a.Longitude) - radians(%(longitude)s)) / 2) ^ 2)) as Distance
                ) d
                    on true
            """

            try:
                radius = float(radius)
            except ValueError as e:
                raise ValueError(f"Radius is not a valid number.")

            whereClause += f"and (a.Latitude is not null) "
            whereClause += f"and (a.Longitude is not null) "
            whereClause += f"and (a.AreaId != 112166257) " # Filter out generic area

            queryParameters["radius"] = radius
            if greaterThan:
                whereClause += f"and (d.Distance >= %(radius)s) "
            else:
                whereClause += f"and (d.Distance <= %(radius)s) "

        return joinClause, whereClause, queryParameters

    def validateKeywordArgs(self, **kwargs) -> None:
        """
        Validates that only the allowed keyword arguments
        are passed

        :param kwargs: Keyword arguments
        :return: None
        """
        allowedKeywords = {
            "parentAreaName",
            "routeDifficultyLow",
            "routeDifficultyHigh",
            "type",
            "height",
            "pitches",
            "grade",
            "severitythreshold",
            "averageRating",
            "elevation",
            "voteCount",
            "city",
            "state",
            "latitude",
            "longitude",
            "radius",
            "proximityRoute",
            "distanceUnits"
        }

        allowedKeywords = set(map(lambda x: x.lower(), allowedKeywords))

        if any(keyword.lower() not in allowedKeywords for keyword in kwargs.keys()):
            invalidKeywords = [keyword for keyword in kwargs.keys() if keyword.lower() not in allowedKeywords]
            raise TypeError(f"Invalid keyword arguments specified for fetchRoutes: {', '.join(invalidKeywords)}.")

    def fetchRoutes(self, **kwargs) -> list:
        """
        This method builds the necessary join and where clauses to gather
        all information as specified by the user in keyword arguments. Note
        that any parameter can be omitted to avoid filtering on it. Some
        parameters are required combinations, such as city/state, or
        latitude/longitude.

        :param parentAreaName: Name of area to find routes under
        :param routeDifficultyLow: Lower bound of route difficulty
        :param routeDifficultyHigh: Upper bound of route difficulty
        :param type: Type of route we are filtering for (Trad, Sport, Aid, etc.)
        :param height: Height to filter on. Append + for >= height, - for <= height.
        :param pitches: Number of pitches to filter on. Append + for >= height, - for <= height.
        :param grade: Grade to filter on (given as 1,2,3,4,5,6,7). Append + for >= height, - for <= height.
        :param severityThreshold: Severity to filter on. This is the maximum severity you will tolerate.
        :param averageRating: Average rating to filter on. Append + for >= height, - for <= height.
        :param elevation: Elevation to filter on. Append + for >= height, - for <= height.
        :param voteCount: Vote count to filter on. Append + for >= height, - for <= height.
        :param city: City to determine route proximity with. Must be specified with state and radius.
        :param state: State to determine route proximity with. Must be specified with city and radius.
        :param latitude: Latitude to determine route proximity with. Must be specified with longitude and radius.
        :param longitude: Longitude to determine route proximity with. Must be specified with latitude and radius.
        :param proximityRoute: URL of the route to determine proximity with. Must be specified with radius.
        :param radius: Radius to used to find routes within. Must be specified with either city/state or latitude/longitude. Append + for >= height, - for <= height.
        :param distanceUnits: Units to use when determining route proximity. Options are "km", "kilometers", "miles", or "mi".
        :return: List of routes meeting the filter conditions
        """
        self.validateKeywordArgs(**kwargs)

        joinClause, whereClause, queryParameters = self.processFilters(**kwargs)

        parentAreaName = kwargs["parentAreaName"] if "parentareaname" in {key.lower() for key in kwargs.keys()} else None

        if parentAreaName is None:
            query = f"""
            select  r.RouteId,
                    r.AreaId,
                    r.RouteName,
                    r.Difficulty_YDS,
                    r.Difficulty_ADL,
                    r.Severity,
                    r.Type,
                    r.Height,
                    r.Pitches,
                    r.Grade,
                    coalesce(r.Description, '') as Description,
                    r.Location,
                    coalesce(r.Protection, '') as Protection,
                    r.FirstAscent,
                    r.FirstAscentYear,
                    r.FirstFreeAscent,
                    r.FirstFreeAscentYear,
                    r.AverageRating,
                    r.VoteCount,
                    r.URL as RouteURL,
                    a0.AreaName,
                    a0.URL as AreaURL,
                    coalesce(string_agg(c.CommentBody, chr(10)||chr(13)||chr(10)||chr(13)), '') as Comments
                from Routes r
                left join RouteComments c
                    on c.RouteId = r.RouteId
                inner join Areas a0
                    on a0.AreaId = r.AreaId
                {joinClause}
                {whereClause}
                group by r.RouteId,
                    r.AreaId,
                    r.RouteName,
                    r.Difficulty_YDS,
                    r.Difficulty_ADL,
                    r.Severity,
                    r.Type,
                    r.Height,
                    r.Pitches,
                    r.Grade,
                    coalesce(r.Description, ''),
                    r.Location,
                    coalesce(r.Protection, ''),
                    r.FirstAscent,
                    r.FirstAscentYear,
                    r.FirstFreeAscent,
                    r.FirstFreeAscentYear,
                    r.AverageRating,
                    r.VoteCount,
                    r.URL,
                    a0.AreaName,
                    a0.URL
                order by r.RouteId;
            """
        else:
            queryParameters["parentAreaName"] = f"%{parentAreaName.lower()}%"
            query = f"""
            ; with recursive SubAreas as (
                select AreaId
                    from Areas
                    where lower(AreaName) like %(parentAreaName)s
                union all
                select a.AreaId
                    from Areas a
                    inner join SubAreas s
                        on s.AreaId = a.ParentAreaId
            )
            select  r.RouteId,
                    r.AreaId,
                    r.RouteName,
                    r.Difficulty_YDS,
                    r.Difficulty_ADL,
                    r.Severity,
                    r.Type,
                    r.Height,
                    r.Pitches,
                    r.Grade,
                    coalesce(r.Description, '') as Description,
                    r.Location,
                    coalesce(r.Protection, '') as Protection,
                    r.FirstAscent,
                    r.FirstAscentYear,
                    r.FirstFreeAscent,
                    r.FirstFreeAscentYear,
                    r.AverageRating,
                    r.VoteCount,
                    r.URL as RouteURL,
                    a0.AreaName,
                    a0.URL as AreaURL,
                    coalesce(string_agg(c.CommentBody, chr(10)||chr(13)||chr(10)||chr(13)), '') as Comments
                from Routes r
                inner join SubAreas s
                    on s.AreaId = r.AreaId
                left join RouteComments c
                    on c.RouteId = r.RouteId
                inner join Areas a0
                    on a0.AreaId = r.AreaId
                {joinClause}
                {whereClause}
                group by r.RouteId,
                    r.AreaId,
                    r.RouteName,
                    r.Difficulty_YDS,
                    r.Difficulty_ADL,
                    r.Severity,
                    r.Type,
                    r.Height,
                    r.Pitches,
                    r.Grade,
                    coalesce(r.Description, ''),
                    r.Location,
                    coalesce(r.Protection, ''),
                    r.FirstAscent,
                    r.FirstAscentYear,
                    r.FirstFreeAscent,
                    r.FirstFreeAscentYear,
                    r.AverageRating,
                    r.VoteCount,
                    r.URL,
                    a0.AreaName,
                    a0.URL
                order by r.RouteId;
            """

        self.cursor.execute(query, queryParameters)
        print(self.cursor.query.decode())

        results = self.cursor.fetchall()

        fields = [
            "RouteId",
            "AreaId",
            "RouteName",
            "Difficulty_YDS",
            "Difficulty_ADL",
            "Severity",
            "Type",
            "Height",
            "Pitches",
            "Grade",
            "Description",
            "Location",
            "Protection",
            "FirstAscent",
            "FirstAscentYear",
            "FirstFreeAscent",
            "FirstFreeAscentYear",
            "AverageRating",
            "VoteCount",
            "RouteURL",
            "AreaName",
            "AreaURL",
            "Comments"
        ]
        fieldCount = len(fields)

        return [{fields[idx] : record[idx] for idx in range(fieldCount)} for record in results]

    def fetchRouteRatings(self, **kwargs) -> list:
        """
        This method fetches user ratings of all routes matching the filter
        conditions passed in by the user

        :param parentAreaName: Name of area to find routes under
        :param routeDifficultyLow: Lower bound of route difficulty
        :param routeDifficultyHigh: Upper bound of route difficulty
        :param type: Type of route we are filtering for (Trad, Sport, Aid, etc.)
        :param height: Height to filter on. Append + for >= height, - for <= height.
        :param pitches: Number of pitches to filter on. Append + for >= height, - for <= height.
        :param grade: Grade to filter on (given as 1,2,3,4,5,6,7). Append + for >= height, - for <= height.
        :param severityThreshold: Severity to filter on. This is the maximum severity you will tolerate.
        :param averageRating: Average rating to filter on. Append + for >= averageRating, - for <= averageRating.
        :param elevation: Elevation to filter on. Append + for >= height, - for <= height.
        :param voteCount: Vote count to filter on. Append + for >= height, - for <= height.
        :param city: City to determine route proximity with. Must be specified with state and radius.
        :param state: State to determine route proximity with. Must be specified with city and radius.
        :param latitude: Latitude to determine route proximity with. Must be specified with longitude and radius.
        :param longitude: Longitude to determine route proximity with. Must be specified with latitude and radius.
        :param proximityRoute: URL of the route to determine proximity with. Must be specified with radius.
        :param radius: Radius to used to find routes within. Must be specified with either city/state or latitude/longitude. Append + for >= height, - for <= height.
        :param distanceUnits: Units to use when determining route proximity. Options are "km", "kilometers", "miles", or "mi".
        :return: List of routes meeting the filter conditions
        """
        self.validateKeywordArgs(**kwargs)

        joinClause, whereClause, queryParameters = self.processFilters(**kwargs)

        parentAreaName = kwargs["parentAreaName"] if "parentareaname" in {key.lower() for key in kwargs.keys()} else None

        if parentAreaName is None:
            query = f"""
            select  r.RouteId,
                    rr.UserId,
                    rr.Rating
                from Routes r
                inner join RouteRatings rr
                    on rr.RouteId = r.RouteId
                    and rr.UserId is not null
                {joinClause}
                {whereClause}
                group by r.RouteId,
                    rr.UserId,
                    rr.Rating,
                    rr.RatingId
                order by r.RouteId,
                    rr.UserId,
                    rr.Rating,
                    rr.RatingId;
            """
        else:
            queryParameters["parentAreaName"] = f"%%{parentAreaName.lower()}%%"
            query = f"""
            ; with recursive SubAreas as (
                select AreaId
                    from Areas
                    where lower(AreaName) like %(parentAreaName)s
                union all
                select a.AreaId
                    from Areas a
                    inner join SubAreas s
                        on s.AreaId = a.ParentAreaId
            )
            select  r.RouteId,
                    rr.UserId,
                    rr.Rating
                from Routes r
                inner join SubAreas s
                    on s.AreaId = r.AreaId
                inner join RouteRatings rr
                    on rr.RouteId = r.RouteId
                    and rr.UserId is not null
                {joinClause}
                {whereClause}
                group by r.RouteId,
                    rr.UserId,
                    rr.Rating,
                    rr.RatingId
                order by r.RouteId,
                    rr.UserId,
                    rr.Rating,
                    rr.RatingId;
            """

        self.cursor.execute(query, queryParameters)
        # print(self.cursor.query.decode())

        results = self.cursor.fetchall()

        fields = [
            "RouteId",
            "UserId",
            "Rating"
        ]

        fieldCount = len(fields)

        return [{fields[idx] : record[idx] for idx in range(fieldCount)} for record in results]

    def fetchUserRoutes(self, userId: int):
        queryParameters = {"userId": userId}
        query = f"""
        select distinct RouteId
            from RouteTicks
            where UserId = %(userId)s
        union
        select distinct RouteId
            from RouteRatings
            where UserId = %(userId)s
        union
        select distinct RouteId
            from RouteToDos
            where UserId = %(userId)s
        """

        self.cursor.execute(query, queryParameters)
        routeIds = self.cursor.fetchall()

        return [{"RouteId": routeId[0]} for routeId in routeIds]


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

    routes = pipe.fetchRouteRatings(
        # city="Boulder",
        # state="Colorado",
        radius=30,
        proximityRoute=r"https://www.mountainproject.com/area/105807692/redgarden-s-buttress",
        severityThreshold="PG13",
        # routeDifficultyLow="5.5",
        # routeDifficultyHigh="5.8",
        # type="Top Rope",
        elevation="5000+",
        parentAreaName="Eldorado Canyon SP",
        routeDifficultyLow="5.8",
        routeDifficultyHigh="5.12a",
        type="Sport, Trad",
        # parentAreaName="Yosemite National Park",
        voteCount="20+",
        averageRating="3.2+"
    )

    # route = pipe.fetchRouteByURL(routeURL=r"https://www.mountainproject.com/route/105924807/the-nose")
    # print(route)
    print(routes)
    # for route in routes:
    #     print(route["RouteName"])
    #     print(" Difficulty:", route["Difficulty_YDS"], route["Difficulty_ADL"])
    #     print(" Type:", route["Type"])
    #     print(" Height:", f"{route['Height']}ft" if route["Height"] is not None else "Not specified")
    #     print(" Pithces:", route["Pitches"] or 1)
    #     print(" Severity:", route["Severity"])
    #     print(" URL:", route["RouteURL"])
        # print(" Description:", route["Description"])
        # print(" Comments:", route["Comments"])
        # print()

    # print(len(routes))
    # for rating in routes:
    #     print(rating["RouteId"])
    #     print(rating["UserId"])
    #     print(rating["Rating"])
    #     print()

    # routesToRecommend = pipe.fetchRoutes(
    #     # routeDifficultyLow="5.8",
    #     # routeDifficultyHigh="5.12a",
    #     # type="Sport, Trad",
    #     parentAreaName="Cob Rock",
    #     voteCount="20+"
    # )
    # routesToRecommend = pipe.fetchRoutesByArea("Cob Rock")

    # print(routesToRecommend)