import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers


class ElasticClient(object):
    def __init__(self, elastic_address='localhost:10000'):
        self.client = Elasticsearch(elastic_address)

    def index_documents(self):
        df = pd.read_csv('./user_ratedmovies.dat', delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
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

    def get_recommended_film_by_user(self, user, index='users'):
        user_id = int(user)
        user_doc = self.client.get_source(index=index, doc_type="user", id=user_id)
        find_films = self.__search_users_by_films(index=index, user=user_doc, user_id=user_id)
        best_film = find_films[0]

        for film in find_films:
            if find_films.count(film) > find_films.count(best_film):
                best_film = film

        return best_film

    def __search_users_by_films(self, index, user, user_id, number_of_films=None):
        if number_of_films is None:
            number_of_films = len(user['ratings'])

        query_params = {
            "query": {
                "query_string": {
                    "default_field": "ratings",
                    "query": ' AND '.join(str(film) for film in user['ratings'][:number_of_films])
                }
            }
        }
        find_films = list()

        for other_user in self.client.search(index=index, body=query_params)['hits']['hits']:
            if other_user['_id'] != user_id:
                find_films.extend(
                    list(film for film in other_user['_source']['ratings'] if film not in user['ratings'])
                )

        if len(find_films) == 0:
            number_of_films -= 1
            return self.__search_users_by_films(index=index, user=user, user_id=user_id,
                                                number_of_films=number_of_films)

        return find_films


def main():
    ec = ElasticClient()
    print(ec.get_recommended_film_by_user(78))


if __name__ == "__main__":
    main()
