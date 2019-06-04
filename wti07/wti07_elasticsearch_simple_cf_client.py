import pandas as pd
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

    def get_recommended_film_by_user(self, user, index='users'):
        user_id = int(user)
        user_doc = self.client.get_source(index=index, doc_type="user", id=user_id)
        find_films = self.__search_film_by_user(index=index, user_doc=user_doc, user_id=user_id)
        best_film = find_films[0]

        for film in find_films:
            if find_films.count(film) > find_films.count(best_film):
                best_film = film

        return best_film

    def get_recommended_user_by_film(self, film, index='movies'):
        film_id = int(film)
        film_doc = self.client.get_source(index=index, doc_type='movie', id=film_id)
        find_users = self.__search_user_by_film(index=index, film_doc=film_doc, film_id=film_id)
        best_user = find_users[0]

        for user in find_users:
            if find_users.count(user) > find_users.count(best_user):
                best_user = user

        return best_user

    def __search_user_by_film(self, index='movies', film_doc=None, film_id=None, number_of_users=None):
        if number_of_users is None:
            number_of_users = len(film_doc['whoRated'])

        query_params = {
            "query": {
                "query_string": {
                    "default_field": "whoRated",
                    "query": ' AND '.join(str(film) for film in film_doc['whoRated'][:number_of_users])
                }
            }
        }
        find_users = list()

        for other_film in self.client.search(index=index, body=query_params)['hits']['hits']:
            if other_film['_id'] != film_id:
                find_users.extend(
                    list(film for film in other_film['_source']['whoRated'] if film not in film_doc['whoRated'])
                )

        if len(find_users) == 0:
            number_of_users -= 1
            return self.__search_user_by_film(index=index, film_doc=film_doc, film_id=film_id,
                                              number_of_users=number_of_users)

        return find_users

    def __search_film_by_user(self, index, user_doc, user_id, number_of_films=None):
        if number_of_films is None:
            number_of_films = len(user_doc['ratings']) - 1

        query_params = {
            "query": {
                "query_string": {
                    "default_field": "ratings",
                    "query": ' AND '.join(str(film) for film in user_doc['ratings'][:number_of_films])
                }
            }
        }
        find_films = list()

        for other_user in self.client.search(index=index, body=query_params)['hits']['hits']:
            if other_user['_id'] != user_id:
                find_films.extend(
                    list(film for film in other_user['_source']['ratings'] if film not in user_doc['ratings'])
                )

        if len(find_films) == 0:
            number_of_films -= 1
            return self.__search_film_by_user(index=index, user_doc=user_doc, user_id=user_id,
                                              number_of_films=number_of_films)

        return find_films


def main():
    ec = ElasticClient()
    print(ec.get_recommended_film_by_user(78))
    print(ec.get_recommended_user_by_film(3))


if __name__ == "__main__":
    main()
