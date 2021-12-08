import os
import pandas as pd
import re

from flask import Flask, render_template, request, jsonify

from RouteDataPipeline import RoutePipeline
from RouteContentRecommender import RouteContentRecommender
# from SparkALSRecommender import SparkALSModel
# from RouteSimilarityRecommender import RouteSimilarityRecommender

from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

pipe = RoutePipeline(
    username=os.getenv("POSTGRESQL_USERNAME"),
    password=os.getenv("POSTGRESQL_PASSWORD"),
    host=os.getenv("POSTGRESQL_HOST"),
    port=os.getenv("POSTGRESQL_PORT"),
    database=os.getenv("POSTGRESQL_DATABASE"),
    geopyUsername=os.getenv("GEOPY_USERAGENT")
)

keywordRecommender = RouteContentRecommender(
    username=os.getenv("POSTGRESQL_USERNAME"),
    password=os.getenv("POSTGRESQL_PASSWORD"),
    host=os.getenv("POSTGRESQL_HOST"),
    port=os.getenv("POSTGRESQL_PORT"),
    database=os.getenv("POSTGRESQL_DATABASE"),
    geopyUsername=os.getenv("GEOPY_USERAGENT")
)


@app.route("/")
def index():
    title = "Route Recommender"
    return render_template("index.html", title=title)


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/contact")
def contact():
    return render_template("contact.html")


@app.route("/select-routes")
def selectRoutes():
    title = "Search Routes"

    difficultySystemValues = pipe.fetchRatingSystemDifficulties()
    difficultySystemValues[""] = []

    return render_template("select-routes.html", title=title, ratingSystems=difficultySystemValues)


@app.route("/filtered-routes", methods=["POST"])
def displayFilteredRoutes():
    parameters = dict()

    labels = [
        "SportCheck",
        "TradCheck",
        "BoulderCheck",
        "AidCheck",
        "AlpineCheck",
        "SnowCheck",
        "IceCheck",
        "MixedCheck",
        "TopRopeCheck"
    ]

    types = [request.form.get(key=label) for label in labels]
    types = ", ".join(routeType for routeType in types if routeType is not None)

    if types:
        parameters["type"] = types

    routeDifficultyLow = request.form.get(key="DifficultyLow")

    if routeDifficultyLow:
        parameters["routeDifficultyLow"] = routeDifficultyLow

    routeDifficultyHigh = request.form.get(key="DifficultyHigh")

    if routeDifficultyHigh:
        parameters["routeDifficultyHigh"] = routeDifficultyHigh

    parentAreaName = request.form.get(key="ParentAreaName")

    if parentAreaName:
        parameters["parentAreaName"] = parentAreaName

    height = request.form.get(key="Height")

    if height:
        parameters["height"] = height

    pitches = request.form.get(key="Pitches")

    if pitches:
        parameters["pitches"] = pitches

    grade = request.form.get(key="Grade")

    if grade:
        parameters["grade"] = grade

    elevation = request.form.get(key="Elevation")

    if elevation:
        parameters["elevation"] = elevation

    severityThreshold = request.form.get(key="Severity")

    if severityThreshold:
        parameters["severityThreshold"] = severityThreshold

    averageRating = request.form.get(key="AverageRating")

    if averageRating:
        parameters["averageRating"] = averageRating

    voteCount = request.form.get(key="RatingCount")

    if voteCount:
        parameters["voteCount"] = voteCount

    city = request.form.get(key="City")
    state = request.form.get(key="State")
    radius = request.form.get(key="Distance")
    distanceUnits = request.form.get(key="DistanceUnits")

    if city and state:
        try:
            float(city)
            float(state)
            useLatLong = True
        except ValueError as e:
            useLatLong = False

        if useLatLong:
            latitude = city
            longitude = state

            parameters["latitude"] = latitude
            parameters["longitude"] = longitude
        else:
            parameters["city"] = city
            parameters["state"] = state

        if radius:
            parameters["radius"] = radius
            parameters["distanceUnits"] = distanceUnits

    if not parentAreaName and not (city and state and radius):
        title = "Search Routes"

        difficultySystemValues = pipe.fetchRatingSystemDifficulties()
        difficultySystemValues[""] = []

        return render_template("select-routes.html",
                               title=title,
                               ratingSystems=difficultySystemValues,
                               errorMessage="Please enter required filtering criteria.")

    if "city" in parameters and "state" in parameters and not pipe.geoAgent.geocode(query=", ".join([city, state]),
                                                                                    exactly_one=True):
        title = "Search Routes"
        difficultySystemValues = pipe.fetchRatingSystemDifficulties()
        difficultySystemValues[""] = []

        return render_template("select-routes.html",
                               title=title,
                               ratingSystems=difficultySystemValues,
                               errorMessage="Ensure that the city and state are correct."
                               )

    routes = pipe.fetchRoutes(**parameters)

    return render_template(
        "filtered-routes.html",
        title="Selected Routes",
        routes=routes
    )


@app.route("/textual-similarity")
def textualSimilarity():
    title = "Textual Similarity"

    difficultySystemValues = pipe.fetchRatingSystemDifficulties()
    difficultySystemValues[""] = []

    return render_template("textual-similarity.html", title=title, ratingSystems=difficultySystemValues)


@app.route("/textually-similar-routes", methods=["POST"])
def displayTextuallySimilarRoutes():
    routeURL = request.form.get(key="Keywords")
    keywords = ""

    if not routeURL:
        title = "Textual Similarity"

        difficultySystemValues = pipe.fetchRatingSystemDifficulties()
        difficultySystemValues[""] = []

        return render_template("textual-similarity.html",
                               title=title,
                               ratingSystems=difficultySystemValues,
                               errorMessage="Please enter a route or keywords to compare to.")

    if re.search(pattern=r"\d+", string=str(routeURL)):
        keywordRecommender.fetchRouteToCompare(routeURL=routeURL)
    else:
        keywords = routeURL


    parameters = dict()

    labels = [
        "SportCheck",
        "TradCheck",
        "BoulderCheck",
        "AidCheck",
        "AlpineCheck",
        "SnowCheck",
        "IceCheck",
        "MixedCheck",
        "TopRopeCheck"
    ]

    types = [request.form.get(key=label) for label in labels]
    types = ", ".join(routeType for routeType in types if routeType is not None)

    if types:
        parameters["type"] = types

    routeDifficultyLow = request.form.get(key="DifficultyLow")

    if routeDifficultyLow:
        parameters["routeDifficultyLow"] = routeDifficultyLow

    routeDifficultyHigh = request.form.get(key="DifficultyHigh")

    if routeDifficultyHigh:
        parameters["routeDifficultyHigh"] = routeDifficultyHigh

    parentAreaName = request.form.get(key="ParentAreaName")

    if parentAreaName:
        parameters["parentAreaName"] = parentAreaName

    height = request.form.get(key="Height")

    if height:
        parameters["height"] = height

    pitches = request.form.get(key="Pitches")

    if pitches:
        parameters["pitches"] = pitches

    grade = request.form.get(key="Grade")

    if grade:
        parameters["grade"] = grade

    elevation = request.form.get(key="Elevation")

    if elevation:
        parameters["elevation"] = elevation

    severityThreshold = request.form.get(key="Severity")

    if severityThreshold:
        parameters["severityThreshold"] = severityThreshold

    averageRating = request.form.get(key="AverageRating")

    if averageRating:
        parameters["averageRating"] = averageRating

    voteCount = request.form.get(key="RatingCount")

    if voteCount:
        parameters["voteCount"] = voteCount

    city = request.form.get(key="City")
    state = request.form.get(key="State")
    radius = request.form.get(key="Distance")
    distanceUnits = request.form.get(key="DistanceUnits")

    if city and state and radius:
        try:
            float(city)
            float(state)
            useLatLong = True
        except ValueError as e:
            useLatLong = False

        if useLatLong:
            latitude = city
            longitude = state

            parameters["latitude"] = latitude
            parameters["longitude"] = longitude
        else:
            parameters["city"] = city
            parameters["state"] = state

        parameters["radius"] = radius
        parameters["distanceUnits"] = distanceUnits

    if not parentAreaName and not (city and state and radius):
        title = "Textual Similarity"

        difficultySystemValues = pipe.fetchRatingSystemDifficulties()
        difficultySystemValues[""] = []

        return render_template("textual-similarity.html",
                               title=title,
                               ratingSystems=difficultySystemValues,
                               errorMessage="Please enter required filtering criteria.")

    if "city" in parameters and "state" in parameters and not pipe.geoAgent.geocode(query=", ".join([city, state]),
                                                                                    exactly_one=True):
        title = "Search Routes"
        difficultySystemValues = pipe.fetchRatingSystemDifficulties()
        difficultySystemValues[""] = []

        return render_template("textual-similarity.html",
                               title=title,
                               ratingSystems=difficultySystemValues,
                               errorMessage="Ensure that the city and state are correct."
                               )

    keywordRecommender.similarityMatrix = None
    keywordRecommender.fetchRoutesToRecommend(**parameters)

    routes = keywordRecommender.recommendRoutes(n=20, keywords=keywords)
    routes["Pitches"].fillna(0, inplace=True)
    routes["Pitches"] = routes["Pitches"].astype(int)

    return render_template(
        "textually-similar-routes.html",
        title="Similar Routes",
        routes=routes
    )

#
# @app.route("/user-similarity")
# def userSimilarity():
#     title = "User Similarity"
#
#     difficultySystemValues = pipe.fetchRatingSystemDifficulties()
#     difficultySystemValues[""] = []
#
#     return render_template("user-similarity.html", title=title, ratingSystems=difficultySystemValues)
#
#
# @app.route("/user-similar-routes", methods=["POST"])
# def displayUserSimilarRoutes():
#     userId = request.form.get(key="UserId")
#
#     if not userId:
#         title = "User Similarity"
#
#         difficultySystemValues = pipe.fetchRatingSystemDifficulties()
#         difficultySystemValues[""] = []
#
#         return render_template("user-similarity.html",
#                                title=title,
#                                ratingSystems=difficultySystemValues,
#                                errorMessage="Please enter a UserId.")
#
#     parameters = dict()
#
#     labels = [
#         "SportCheck",
#         "TradCheck",
#         "BoulderCheck",
#         "AidCheck",
#         "AlpineCheck",
#         "SnowCheck",
#         "IceCheck",
#         "MixedCheck",
#         "TopRopeCheck"
#     ]
#
#     types = [request.form.get(key=label) for label in labels]
#     types = ", ".join(routeType for routeType in types if routeType is not None)
#
#     if types:
#         parameters["type"] = types
#
#     routeDifficultyLow = request.form.get(key="DifficultyLow")
#
#     if routeDifficultyLow:
#         parameters["routeDifficultyLow"] = routeDifficultyLow
#
#     routeDifficultyHigh = request.form.get(key="DifficultyHigh")
#
#     if routeDifficultyHigh:
#         parameters["routeDifficultyHigh"] = routeDifficultyHigh
#
#     parentAreaName = request.form.get(key="ParentAreaName")
#
#     if parentAreaName:
#         parameters["parentAreaName"] = parentAreaName
#
#     height = request.form.get(key="Height")
#
#     if height:
#         parameters["height"] = height
#
#     pitches = request.form.get(key="Pitches")
#
#     if pitches:
#         parameters["pitches"] = pitches
#
#     grade = request.form.get(key="Grade")
#
#     if grade:
#         parameters["grade"] = grade
#
#     elevation = request.form.get(key="Elevation")
#
#     if elevation:
#         parameters["elevation"] = elevation
#
#     severityThreshold = request.form.get(key="Severity")
#
#     if severityThreshold:
#         parameters["severityThreshold"] = severityThreshold
#
#     averageRating = request.form.get(key="AverageRating")
#
#     if averageRating:
#         parameters["averageRating"] = averageRating
#
#     voteCount = request.form.get(key="RatingCount")
#
#     if voteCount:
#         parameters["voteCount"] = voteCount
#
#     city = request.form.get(key="City")
#     state = request.form.get(key="State")
#     radius = request.form.get(key="Distance")
#     distanceUnits = request.form.get(key="DistanceUnits")
#
#     if city and state and radius:
#         try:
#             float(city)
#             float(state)
#             useLatLong = True
#         except ValueError as e:
#             useLatLong = False
#
#         if useLatLong:
#             latitude = city
#             longitude = state
#
#             parameters["latitude"] = latitude
#             parameters["longitude"] = longitude
#         else:
#             parameters["city"] = city
#             parameters["state"] = state
#
#         parameters["radius"] = radius
#         parameters["distanceUnits"] = distanceUnits
#
#     if not parentAreaName and not (city and state and radius):
#         title = "User Similarity"
#
#         difficultySystemValues = pipe.fetchRatingSystemDifficulties()
#         difficultySystemValues[""] = []
#
#         return render_template("user-similarity.html",
#                                title=title,
#                                ratingSystems=difficultySystemValues,
#                                errorMessage="Please enter required filtering criteria.")
#
#     if "city" in parameters and "state" in parameters and not pipe.geoAgent.geocode(query=", ".join([city, state]),
#                                                                                     exactly_one=True):
#         title = "Search Routes"
#         difficultySystemValues = pipe.fetchRatingSystemDifficulties()
#         difficultySystemValues[""] = []
#
#         return render_template("user-similarity.html",
#                                title=title,
#                                ratingSystems=difficultySystemValues,
#                                errorMessage="Ensure that the city and state are correct."
#                                )
#
#     routes = userRecommender.recommendRoutes(
#         n=20,
#         userId=int(userId),
#         **parameters
#     )
#
#     routes["Pitches"].fillna(0, inplace=True)
#     routes["Pitches"] = routes["Pitches"].astype(int)
#
#     return render_template(
#         "user-similar-routes.html",
#         title="Similar Routes",
#         routes=routes
#     )
#
#
# @app.route("/item-similarity")
# def itemSimilarity():
#     title = "Route Similarity"
#
#     difficultySystemValues = pipe.fetchRatingSystemDifficulties()
#     difficultySystemValues[""] = []
#
#     return render_template("item-similarity.html", title=title, ratingSystems=difficultySystemValues)
#
#
# @app.route("/item-similar-routes", methods=["POST"])
# def displayItemSimilarRoutes():
#     routeURL = request.form.get(key="RouteURL")
#
#     if not routeURL:
#         title = "Route Similarity"
#
#         difficultySystemValues = pipe.fetchRatingSystemDifficulties()
#         difficultySystemValues[""] = []
#
#         return render_template("item-similarity.html",
#                                title=title,
#                                ratingSystems=difficultySystemValues,
#                                errorMessage="Please enter a route to compare to.")
#
#     routeSimilarityRecommender.fetchRouteToCompare(routeURL=routeURL)
#
#     parameters = dict()
#
#     labels = [
#         "SportCheck",
#         "TradCheck",
#         "BoulderCheck",
#         "AidCheck",
#         "AlpineCheck",
#         "SnowCheck",
#         "IceCheck",
#         "MixedCheck",
#         "TopRopeCheck"
#     ]
#
#     types = [request.form.get(key=label) for label in labels]
#     types = ", ".join(routeType for routeType in types if routeType is not None)
#
#     if types:
#         parameters["type"] = types
#
#     routeDifficultyLow = request.form.get(key="DifficultyLow")
#
#     if routeDifficultyLow:
#         parameters["routeDifficultyLow"] = routeDifficultyLow
#
#     routeDifficultyHigh = request.form.get(key="DifficultyHigh")
#
#     if routeDifficultyHigh:
#         parameters["routeDifficultyHigh"] = routeDifficultyHigh
#
#     parentAreaName = request.form.get(key="ParentAreaName")
#
#     if parentAreaName:
#         parameters["parentAreaName"] = parentAreaName
#
#     height = request.form.get(key="Height")
#
#     if height:
#         parameters["height"] = height
#
#     pitches = request.form.get(key="Pitches")
#
#     if pitches:
#         parameters["pitches"] = pitches
#
#     grade = request.form.get(key="Grade")
#
#     if grade:
#         parameters["grade"] = grade
#
#     elevation = request.form.get(key="Elevation")
#
#     if elevation:
#         parameters["elevation"] = elevation
#
#     severityThreshold = request.form.get(key="Severity")
#
#     if severityThreshold:
#         parameters["severityThreshold"] = severityThreshold
#
#     averageRating = request.form.get(key="AverageRating")
#
#     if averageRating:
#         parameters["averageRating"] = averageRating
#
#     voteCount = request.form.get(key="RatingCount")
#
#     if voteCount:
#         parameters["voteCount"] = voteCount
#
#     city = request.form.get(key="City")
#     state = request.form.get(key="State")
#     radius = request.form.get(key="Distance")
#     distanceUnits = request.form.get(key="DistanceUnits")
#
#     if city and state and radius:
#         try:
#             float(city)
#             float(state)
#             useLatLong = True
#         except ValueError as e:
#             useLatLong = False
#
#         if useLatLong:
#             latitude = city
#             longitude = state
#
#             parameters["latitude"] = latitude
#             parameters["longitude"] = longitude
#         else:
#             parameters["city"] = city
#             parameters["state"] = state
#
#         parameters["radius"] = radius
#         parameters["distanceUnits"] = distanceUnits
#
#     if not parameters:
#         title = "Route Similarity"
#
#         difficultySystemValues = pipe.fetchRatingSystemDifficulties()
#         difficultySystemValues[""] = []
#
#         return render_template("item-similarity.html",
#                                title=title,
#                                ratingSystems=difficultySystemValues,
#                                errorMessage="Please enter some filtering criteria.")
#
#     if "city" in parameters and "state" in parameters and not pipe.geoAgent.geocode(query=", ".join([city, state]),
#                                                                                     exactly_one=True):
#         title = "Search Routes"
#         difficultySystemValues = pipe.fetchRatingSystemDifficulties()
#         difficultySystemValues[""] = []
#
#         return render_template("item-similarity.html",
#                                title=title,
#                                ratingSystems=difficultySystemValues,
#                                errorMessage="Ensure that the city and state are correct."
#                                )
#
#     routes = routeSimilarityRecommender.recommendRoutes(
#         n=20,
#         **parameters
#     )
#
#     routes["Pitches"].fillna(0, inplace=True)
#     routes["Pitches"] = routes["Pitches"].astype(int)
#
#     return render_template(
#         "item-similar-routes.html",
#         title="Similar Routes",
#         routes=routes
#     )


def main():
    app.run(debug=False, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
