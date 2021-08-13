import numpy as np
import pandas as pd
import os
import pyspark as ps
import pickle

from dotenv import load_dotenv

from sklearn.model_selection import train_test_split

from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

from src.RouteDataPipeline import RoutePipeline

spark = ps.sql.SparkSession.builder.master("local[8]").appName("SparkALSRecommender").getOrCreate()


class SparkALSModel(object):
    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 port: str,
                 database: str,
                 geopyUsername: str):
        """

        :param username: PostgreSQL username
        :param password: PostgreSQL password
        :param host: PostgreSQL host
        :param port: PostgreSQL port
        :param database: PostgreSQL database
        :param geopyUsername: geopy user_agent
        """

        self.routePipeline = RoutePipeline(
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            geopyUsername=geopyUsername
        )

        self.userRatings = None
        self.model = None
        self.recommender = None

    def load(self, dir: str) -> None:
        """
        Loads the appropriately fit models and the user ratings

        :param filepath: Path to directory containing the saved models
        """
        self.model = ALS.load(dir + ("/" if dir[-1] != "/" else "") + "model")
        self.recommender = ALSModel.load(dir + ("/" if dir[-1] != "/" else "") + "recommender")

        with open(dir + ("/" if dir[-1] != "/" else "") + "ratings.p", "rb") as file:
            self.userRatings = pickle.load(file)


    def save(self, dir: str) -> None:
        """
        This method saves an instance of this class as a pickle file

        :param filepath: Location to save the class
        """
        self.model.save(dir + ("/" if dir[-1] != "/" else "") + "model")
        self.recommender.save(dir + ("/" if dir[-1] != "/" else "") + "recommender")

        with open(dir + ("/" if dir[-1] != "/" else "") + "ratings.p", "wb") as file:
            pickle.dump(self.userRatings, file)

    def fetchRatingData(self, **kwargs) -> pd.DataFrame:
        """
        This method fetches the user ratings of routes matching the specified filters

        :param parentAreaName: Name of area to find routes under
        :param routeDifficultyLow: Lower bound of route difficulty
        :param routeDifficultyHigh: Upper bound of route difficulty
        :param type: Type of route we are filtering for (Trad, Sport, Aid, etc.)
        :param height: Height to filter on. Append + for >= height, - for <= height.
        :param pitches: Number of pitches to filter on. Append + for >= height, - for <= height.
        :param grade: Grade to filter on (given as 1,2,3,4,5,6,7). Append + for >= grade, - for <= grade.
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
        self.userRatings = pd.DataFrame(self.routePipeline.fetchRouteRatings(**kwargs))

        return self.userRatings

    def constructModelViaCrossValidation(self,
                                         rank: list = [10, 50, 100, 150],
                                         regParam: list = [0.15, 0.1, 0.05, 0.01],
                                         metricName: str = "rmse",
                                         numFolds: int = 5,
                                         parallelism: int = 1) -> tuple:
        """
        This method constructs a fitted ALS model by grid search cross-validation over the supplied parameters

        :param rank: List of allowed ALS.rank values
        :param regParam: List of allow ALS.regParam values
        :param metricName: Metric to use to evaluate the model
        :param numFolds: Number of folds to use for K-fold cross-validations
        :param parallelism: Degree of parallelism to use
        :return: The best model found by validating and the RMSE it obtained
        """
        if self.userRatings is None:
            raise ValueError("Error: No user rating data is present. Run fetchRatingData with the appropriate "
                             "filtering parameters to populate user ratings.")

        sparkALS = ALS(
            userCol="UserId",
            itemCol="RouteId",
            ratingCol="Rating",
            nonnegative=True,
            implicitPrefs=False,
            coldStartStrategy="drop"
        )

        parameterGrid = ParamGridBuilder()
        parameterGrid.addGrid(sparkALS.rank, rank)
        parameterGrid.addGrid(sparkALS.regParam, regParam)
        parameterGrid = parameterGrid.build()

        modelEvaluator = RegressionEvaluator(
            metricName=metricName,
            labelCol="Rating",
            predictionCol="prediction"
        )

        crossValidator = CrossValidator(
            estimator=sparkALS,
            estimatorParamMaps=parameterGrid,
            evaluator=modelEvaluator,
            numFolds=numFolds,
            parallelism=parallelism
        )

        ratingsTrain, ratingsTest = train_test_split(self.userRatings, test_size=0.8)

        train = spark.createDataFrame(data=ratingsTrain)
        test = spark.createDataFrame(data=ratingsTest)

        model = crossValidator.fit(train)

        self.recommender = model.bestModel

        predictions = self.recommender.transform(test)
        metricValue = modelEvaluator.evaluate(predictions)

        return self.model, metricValue

    def constructModel(self,
                       regParam: float = 0.15,
                       rank: int = 150) -> ps.ml.recommendation.ALSModel:
        """
        Construct a spark ALS model

        :param regParam: Regularization parameter
        :param rank: Rank parameter
        :return: The (untrained) spark ALS model
        """

        if self.userRatings is None:
            raise ValueError("Error: No user rating data is present. Run fetchRatingData with the appropriate "
                             "filtering parameters to populate user ratings.")

        sparkALS = ALS(
            userCol="UserId",
            itemCol="RouteId",
            ratingCol="Rating",
            nonnegative=True,
            implicitPrefs=False,
            coldStartStrategy="drop",
            regParam=regParam,
            rank=rank
        )

        self.model = sparkALS

        return self.model

    def fit(self, data: ps.sql.session.SparkSession = None) -> ps.ml.recommendation.ALSModel:
        """
        Fit the model to data

        :param data: Spark dataframe containing data to fit to. Defaults to entire set of user ratings
        :return: Trained model
        """
        if data is None:
            if self.userRatings is not None:
                data = spark.createDataFrame(self.userRatings)
            else:
                raise ValueError("Error: No user rating data is present. Run fetchRatingData with the appropriate "
                             "filtering parameters to populate user ratings.")

        self.recommender = self.model.fit(data)

        return self.recommender

    def recommendRoutes(self, userId: int, n: int = 5, excludeInteractedRoutes: bool = True, **kwargs) -> pd.DataFrame:
        """
        Recommend routes based on user id

        :param userId: User id of the user the recommend rotues for
        :param n: How many routes to recommend
        :param excludeInteractedRoutes: Whether or not to exlude routes the user has interacted with (ticked, rated, or marked to-do)
        :param kwargs: Key word arguments to filter the routes. See RouteDataPipeline.fetchRoutes
        :return: Top n recommended routes
        """
        routesToRecommend = pd.DataFrame(
            self.routePipeline.fetchRoutes(**kwargs)
        )

        if routesToRecommend.empty:
            raise ValueError("Error: No routes found matching supplied search criteria")

        if excludeInteractedRoutes:
            userInteractedRoutes = pd.DataFrame(self.routePipeline.fetchUserRoutes(userId=userId))
            routesToRecommend = routesToRecommend[~routesToRecommend["RouteId"].isin(userInteractedRoutes["RouteId"])]

        routeIds = routesToRecommend.copy()[["RouteId"]]
        routeIds["UserId"] = [userId] * len(routesToRecommend)

        spark_df = spark.createDataFrame(routeIds)
        recommendations = self.recommender.transform(spark_df).toPandas()
        recommendations["prediction"].fillna(0, inplace=True)

        routesToRecommend = routesToRecommend.merge(recommendations[["RouteId", "prediction"]], on="RouteId", how="inner")
        recommendationIdxs = routesToRecommend["prediction"].values.argsort()

        return routesToRecommend.iloc[recommendationIdxs[-1:-n:-1]]


def main():
    load_dotenv()

    model = SparkALSModel(
        username="postgres",
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host="127.0.0.1",
        port="5432",
        database="MountainProject",
        geopyUsername="zsnyder21"
    )

    print("Fetching rating data...")

    model.fetchRatingData(
        type="Sport, Trad",
        parentAreaName="Boulder",
        voteCount="20+"
    )

    print("Data fetch complete.")
    print()
    print(model.userRatings.head())
    print()
    print("Building model based on cross-validation parameters")

    bestModel, metricValue = model.constructModelViaCrossValidation(
        rank=[150, 200],
        regParam=[0.15, 0.2],
        metricName="rmse",
        numFolds=3,
        parallelism=6
    )

    print("Model construction complete.")
    print(" Rank:, ", bestModel._java_obj.parent().getRank())
    print(" regParam: ", bestModel._java_obj.parent().getRegParam())
    print(" RMSE: ", metricValue)


if __name__ == "__main__":
    main()




