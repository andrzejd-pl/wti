import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers


class ElasticClient(object):
    def __init__(self, elastic_address='localhost:10000'):
        self.client = Elasticsearch(elastic_address)

    def index_documents(self):
        df = pd.read_csv('../data/user_ratedmovies.dat', delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
        means = df.groupby(['userID'], as_index=False, sort=False) \
                    .mean() \
                    .loc[:, ['userID', 'rating']] \
            .rename(columns={'rating': 'ratingMean'})
        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']
        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']] \
            .rename(columns={'ratingNormal': 'rating'}) \
            .pivot_table(index='userID', columns='movieID', values='rating') \
            .fillna(0)
        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0].sort_values(ascending=False).index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.client, index_users)
        print("Done")
        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] > 0].sort_values(ascending=False).index.values.tolist()
            }
        } for column in ratings]
        helpers.bulk(self.client, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.client.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.client.get(index=index, doc_type="movie", id=movie_id)["_source"]


def main():
    ec = ElasticClient()
    ec.index_documents()

    # ------ Simple operations ------
    print()
    user_document = ec.get_movies_liked_by_user(75)
    movie_id = np.random.choice(user_document['ratings'])
    movie_document = ec.get_users_that_like_movie(movie_id)
    random_user_id = np.random.choice(movie_document['whoRated'])
    random_user_document = ec.get_movies_liked_by_user(random_user_id)

    print('User 75 likes following movies:')
    print(user_document)

    print('Movie {} is liked by following users:'.format(movie_id))
    print(movie_document)

    print('Is user 75 among users in movie {} document?'.format(movie_id))
    print(movie_document['whoRated'].index(75) != -1)

    import random

    some_test_movie_id = 1
    print("Some test movie ID: ", some_test_movie_id)
    list_of_users_who_liked_movie_of_given_id = ec.get_users_that_like_movie(some_test_movie_id)["whoRated"]
    print("List of users who liked the test movie: ", *list_of_users_who_liked_movie_of_given_id)
    index_of_random_user_who_liked_movie_of_given_id = random.randint(0, len(list_of_users_who_liked_movie_of_given_id))

    print("Index of random user who liked the test movie: ",
          index_of_random_user_who_liked_movie_of_given_id)
    some_test_user_id = list_of_users_who_liked_movie_of_given_id[index_of_random_user_who_liked_movie_of_given_id]

    print("ID of random user who liked the test movie: ", some_test_user_id)
    movies_liked_by_user_of_given_id = ec.get_movies_liked_by_user(some_test_user_id)["ratings"]

    print("IDs of movies liked by the random user who liked the test movie: ", *movies_liked_by_user_of_given_id)

    if some_test_movie_id in movies_liked_by_user_of_given_id:
        print("As expected, the test movie ID is among the IDs of movies " +
              "liked by the random user who liked the test movie ;-)")


if __name__ == "__main__":
    main()
