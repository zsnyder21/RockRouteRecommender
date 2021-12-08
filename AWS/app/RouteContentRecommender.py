
import pandas as pd
import os
import numpy as np

from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from nltk.tokenize import RegexpTokenizer

from RouteDataPipeline import RoutePipeline


class RouteContentRecommender(object):
    """
    This class is responsible for making recommendations based on route content
    """
    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 port: str,
                 database: str,
                 geopyUsername: str,
                 vocabulary: list = None,
                 maxFeatures: int = 25,
                 nGramRange: tuple = (1, 2),
                 similarityMetric=None):
        """

        :param username: PostgreSQL username
        :param password: PostgreSQL password
        :param host: PostgreSQL host
        :param port: PostgreSQL port
        :param database: PostgreSQL database
        :param geopyUsername: geopy user_agent
        :param vocabulary: Iterable of words to compare routes with
        :param maxFeatures: Number of features to use when comparing routes
        :param nGramRange: Tuple containing minimum/maximum length of n grams
        :param similarityMetric: Metric to use when computing route similarity
        """

        self.routePipeline = RoutePipeline(
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            geopyUsername=geopyUsername
        )

        self.routes = None

        if vocabulary is None:
            self.vocabulary = np.array([
                "crack",
                "splitter",
                "tips",
                "finger",
                "fingers",
                "fingerlock",
                "ringlocks",
                "thin",
                "hand",
                "hands",
                "jam",
                "jams",
                "jugs"
                "handjam",
                "handjams",
                "wide",
                "fist",
                "fists",
                "fistjam",
                "fistjams",
                "butterflies",
                "stack",
                "stacks",
                "chicken",
                "wing",
                "arm",
                "bar",
                "arm-bar",
                "squeeze",
                "chimney",
                "ow",
                "offwidth",
                "off-width",
                "pocket",
                "face",
                "vertical",
                "overhang",
                "steep",
                "traverse",
                "slab",
                "friction",
                "crimp",
                "dihedral",
                "arete",
                "roof",
                "lieback",
                "layback",
                "lay-back",
                "mantel",
                "mantle",
                "sloper",
                "dyno",
                "stem",
                "corner",
                "pumpy",
                "finger jam",
                "finger lock",
                "ring lock",
                "hand jam",
                "fist jam",
                "squeeze chimney",
                "arm bar",
                "toe hook",
                "heel hook",
                "over roof",
                "through roof",
                "pull roof",
                "thin crack",
                "hand crack",
                "fist crack",
                "wide crack",
                "offwidth crack",
                "off-width crack"
            ])
        else:
            self.vocabulary = np.array(vocabulary)

        self.vectorizer = TfidfVectorizer(
            tokenizer=self.tokenize,
            max_features=maxFeatures,
            ngram_range=nGramRange,
            vocabulary=self.vocabulary
        )

        self.docTermMatrix = None
        self.similarityMatrix = None

        if similarityMetric is None:
            self.similarityMetric = cosine_similarity
        else:
            self.similarityMetric = similarityMetric

        self.routeToCompare = None
        self.routesToRecommend = None
        self.routes = None

    @staticmethod
    def tokenize(text: str) -> list:
        """
        This method tokenizes the text for use in the vectorizer

        :param text: Text to be tokenized
        :return: List of tokenized text
        """
        tokenizer = RegexpTokenizer(pattern=r"[\w']+")
        return tokenizer.tokenize(text)


    def fetchRouteToCompare(self, routeURL: str) -> dict:
        """
        This method fetches the route we are finding similar routes to

        :param routeURL: URL of the route we are recommending based off of
        :return: Dictionary representation of the route being compared to
        """
        self.routeToCompare = self.routePipeline.fetchRouteByURL(routeURL=routeURL)

        return self.routeToCompare

    def fetchRoutesToRecommend(self, **kwargs) -> list:
        """
         This method fetches the routes we are interested in recommending to the user

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
        self.routesToRecommend = self.routePipeline.fetchRoutes(**kwargs)

        if not self.routesToRecommend:
            raise ValueError("Error: No routes match the supplied filtering criteria")

        return self.routesToRecommend

    def fit(self, keywords: str = None) -> None:
        """
        Fit the vectorizer and determine similarity. Called automatically from recommendRoutes if necessary

        """
        if self.routeToCompare:
            self.routes = pd.DataFrame(
                [self.routeToCompare]
                + [route for route in self.routesToRecommend if route["RouteId"] != self.routeToCompare["RouteId"]]
            )
        else:
            if keywords is None:
                raise ValueError("Error: Either a route to compare to or keywords to compare to are required.")

            wordsToCompare = {key: "" for key in self.routesToRecommend[0].keys()}
            wordsToCompare["Description"] = keywords

            self.routes = pd.DataFrame([wordsToCompare] + self.routesToRecommend)

        self.routes["Text"] = self.routes["Description"] + self.routes["Protection"] + self.routes["Comments"]

        self.docTermMatrix = self.vectorizer.fit_transform(self.routes["Text"])

        self.similarityMatrix = self.similarityMetric(self.docTermMatrix[0], self.docTermMatrix)
        self.routes["SimilarityScore"] = self.similarityMatrix[0, :]

        self.similarityMatrix = self.similarityMatrix.argsort()

    def recommendRoutes(self, n: int = 5, keywords: str = None) -> pd.DataFrame:
        """
        Recommend top n similar routes

        :param n: Number of routes to recommend
        :param keywords: Key words to use to compare route similarity
        :return: Pandas DataFrame object holding the recommended routes
        """
        if self.similarityMatrix is None:
            self.fit(keywords=keywords)

        similarityIndices = self.similarityMatrix[0, -2:-(n + 2):-1]

        return self.routes.iloc[similarityIndices]


def main():
    load_dotenv()

    recommender = RouteContentRecommender(
        username=os.getenv("POSTGRESQL_USERNAME"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host=os.getenv("POSTGRESQL_HOST"),
        port=os.getenv("POSTGRESQL_PORT"),
        database=os.getenv("POSTGRESQL_DATABASE"),
        geopyUsername=os.getenv("GEOPY_USERAGENT")
    )

    # recommender.fetchRouteToCompare(
    #     # routeURL=r"https://www.mountainproject.com/route/105862912/serenity-crack"
    #     routeURL=r"https://www.mountainproject.com/route/105991737/freeblast"
    #     # routeURL=r"https://www.mountainproject.com/route/105748096/aid-crack"
    #     # routeURL=r"https://www.mountainproject.com/route/105749647/upside-the-cranium"
    #     # routeURL=r"https://www.mountainproject.com/route/105867013/commitment"
    # )

    recommender.fetchRoutesToRecommend(
        severityThreshold="PG13",
        # routeDifficultyLow="5.13c",
        # routeDifficultyHigh="5.14d",
        # type="Sport",
        parentAreaName="Eldorado Canyon SP",
        # voteCount="20+",
        # averageRating="3.0+"
    )

    recommendations = recommender.recommendRoutes(n=5, keywords="Roof, slab")
    print(recommendations[["RouteName", "SimilarityScore"]])


if __name__ == "__main__":
    main()

