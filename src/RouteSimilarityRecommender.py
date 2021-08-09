import os
import pandas as pd
import numpy as np

from dotenv import load_dotenv

from surprise import Dataset, Reader, KNNWithZScore

from src.RouteDataPipeline import RoutePipeline


class RouteSimilarityRecommender(object):
    """
    This class recommends climbs based on what users who climbed a reference climbed have also climbed.
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

        self.routePipeline = RoutePipeline(
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            geopyUsername=geopyUsername
        )

        self.routeToCompare = None


    def fetchRouteToCompare(self, routeURL: str) -> dict:
        """
        This method fetches the route we are finding similar routes to

        :param routeURL: URL of the route we are recommending based off of
        :return: Dictionary representation of the route being compared to
        """
        self.routeToCompare = self.routePipeline.fetchRouteRatingsByURL(routeURL=routeURL)

        return self.routeToCompare

    def recommendRoutes(self, n: int = 5, **kwargs) -> pd.DataFrame:
        """
        Recommend routes based on user ratings of similar climbs

        :param n: How many routes to recommend
        :param kwargs: Key word arguments to filter the routes. See RouteDataPipeline.fetchRoutes
        :return: Top n recommended routes
        """
        if self.routeToCompare is None:
            raise ValueError("Error: No route selected to compare to. Run fetchRouteToCompare to select one.")


        routesToRecommend = pd.DataFrame(
            self.routePipeline.fetchRoutes(**kwargs)
        )

        if routesToRecommend.empty:
            raise ValueError("Error: No routes found matching supplied search criteria")

        routesToCompare = self.routePipeline.fetchRouteRatings(**kwargs)

        routes = pd.DataFrame(
            self.routeToCompare + [
                {
                    "RouteId": route["RouteId"],
                    "UserId": route["UserId"],
                    "Rating": route["Rating"]
                }
            for route in routesToCompare]
        )

        reader = Reader(rating_scale=(0,4))
        data = Dataset.load_from_df(df=routes, reader=reader)

        algorithm = KNNWithZScore(
            k=50,
            verbose=False,
            sim_options={
                "name": "cosine",
                "user_based": False,
                "min_support": 2
            }
        )

        trainset = data.build_full_trainset()
        algorithm.fit(trainset)

        innerRouteIds = algorithm.get_neighbors(
            iid=trainset.to_inner_uid(ruid=self.routeToCompare[0]["RouteId"]),
            k=min(n, len(routesToRecommend))
        )

        routeIds = list(map(lambda id: trainset.to_raw_uid(id), innerRouteIds))

        return routesToRecommend[routesToRecommend["RouteId"].isin(routeIds)]
