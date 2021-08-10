import os
import pandas as pd
import re

from flask import Flask, render_template, request, jsonify

from src.RouteDataPipeline import RoutePipeline
from src.RouteContentRecommender import RouteContentRecommender
from src.SparkALSRecommender import SparkALSModel
from src.RouteSimilarityRecommender import RouteSimilarityRecommender

from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

pipe = RoutePipeline(
    username="postgres",
    password=os.getenv("POSTGRESQL_PASSWORD"),
    host="127.0.0.1",
    port="5432",
    database="MountainProject",
    geopyUsername="zsnyder21"
)

keywordRecommender = RouteContentRecommender(
    username="postgres",
    password=os.getenv("POSTGRESQL_PASSWORD"),
    host="127.0.0.1",
    port="5432",
    database="MountainProject",
    geopyUsername="zsnyder21"
)

userRecommender = SparkALSModel(
        username="postgres",
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host="127.0.0.1",
        port="5432",
        database="MountainProject",
        geopyUsername="zsnyder21"
    )

userRecommender.load(dir="../models/SparkALS/")

routeSimilarityRecommender = RouteSimilarityRecommender(
    username="postgres",
    password=os.getenv("POSTGRESQL_PASSWORD"),
    host="127.0.0.1",
    port="5432",
    database="MountainProject",
    geopyUsername="zsnyder21"
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
    title = "Select Filtering Criteria"

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

    routes = pipe.fetchRoutes(**parameters)

    return render_template(
        "filtered-routes.html",
        routes=routes
    )

@app.route("/textual-similarity")
def textualSimilarity():
    title = "Select Filtering Criteria"

    difficultySystemValues = pipe.fetchRatingSystemDifficulties()
    difficultySystemValues[""] = []

    return render_template("textual-similarity.html", title=title, ratingSystems=difficultySystemValues)

@app.route("/textually-similar-routes", methods=["POST"])
def displayTextuallySimilarRoutes():
    routeURL = request.form.get(key="Keywords")
    keywords = ""

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

    keywordRecommender.similarityMatrix = None
    keywordRecommender.fetchRoutesToRecommend(**parameters)

    routes = keywordRecommender.recommendRoutes(n=20, keywords=keywords)
    routes["Pitches"].fillna(0, inplace=True)
    routes["Pitches"] = routes["Pitches"].astype(int)

    return render_template(
        "textually-similar-routes.html",
        routes=routes
    )


@app.route("/user-similarity")
def userSimilarity():
    title = "Select Filtering Criteria"

    difficultySystemValues = pipe.fetchRatingSystemDifficulties()
    difficultySystemValues[""] = []

    return render_template("user-similarity.html", title=title, ratingSystems=difficultySystemValues)

@app.route("/user-similar-routes", methods=["POST"])
def displayUserSimilarRoutes():
    userId = request.form.get(key="UserId")

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

    routes = userRecommender.recommendRoutes(
        n=20,
        userId=200552300,
        **parameters
    )

    routes["Pitches"].fillna(0, inplace=True)
    routes["Pitches"] = routes["Pitches"].astype(int)

    return render_template(
        "user-similar-routes.html",
        routes=routes
    )


@app.route("/item-similarity")
def itemSimilarity():
    title = "Select Filtering Criteria"

    difficultySystemValues = pipe.fetchRatingSystemDifficulties()
    difficultySystemValues[""] = []

    return render_template("item-similarity.html", title=title, ratingSystems=difficultySystemValues)

@app.route("/item-similar-routes", methods=["POST"])
def displayItemSimilarRoutes():
    routeURL = request.form.get(key="RouteURL")

    routeSimilarityRecommender.fetchRouteToCompare(routeURL=routeURL)

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

    routes = routeSimilarityRecommender.recommendRoutes(
        n=20,
        **parameters
    )

    routes["Pitches"].fillna(0, inplace=True)
    routes["Pitches"] = routes["Pitches"].astype(int)

    return render_template(
        "item-similar-routes.html",
        routes=routes
    )


def main():
    app.run(host="0.0.0.0")


if __name__ == "__main__":
    main()
