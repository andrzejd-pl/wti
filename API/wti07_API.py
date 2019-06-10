import json

import flask
from flask import Response

from wti06 import wti06_ETL
from wti05.wti05_module import FlaskAppWrapper
from wti07.wti07_elasticsearch_simple_cf_client import ElasticClient


class APILogic(object):
    def __init__(self, app=FlaskAppWrapper(name='wti-06')):
        self.app = app
        self.es = ElasticClient()

    def get_user(self, user_id):
        return Response(status=200, response=json.dumps(self.es.get_movies_liked_by_user(user_id)),
                        mimetype='application/json')

    def get_movie(self, movie_id):
        return Response(status=200, response=json.dumps(self.es.get_users_that_like_movie(movie_id)),
                        mimetype='application/json')

    def get_cf_by_user(self, user_id):
        return Response(status=200, response=json.dumps(self.es.get_recommended_movie_by_user(user_id)),
                        mimetype='application/json')

    def get_cf_by_movie(self, movie_id):
        return Response(status=200, response=json.dumps(self.es.get_recommended_user_by_movie(movie_id)),
                        mimetype='application/json')

    def add_user(self):
        data = json.loads(flask.request.data)
        user_id = data['user_id']
        self.es.add_user(user_id=user_id, user=list(rating for rating in data['ratings']))
        self.client.push_data_table(self.table, data)
        return Response(status=201)

    def add_row(self):
        data = json.loads(flask.request.data)
        data['user_ratings_id'] = self.next_index
        self.next_index += 1
        self.client.push_data_table(self.table, data)
        return Response(status=201)

    def get_all(self):
        all_rows = []
        for x in self.client.get_data_table(self.table):
            all_rows.append(dict(x))

        return Response(status=200, response=json.dumps(all_rows),
                        mimetype='application/json')

    def delete(self):
        self.client.clear_table(self.table)

        return Response(status=204)

    def get_all_avg_users(self):
        all_rows = []
        for x in self.client.get_data_table(self.table):
            all_rows.append(dict(x))

        return flask.Response(response=json.dumps(wti06_ETL.count_avg(all_rows)), status=200,
                              mimetype='application/json')

    def get_avg_user(self, user):
        all_rows = []
        for x in self.client.get_data_table_per_column_value(self.table, 'user_id', user):
            all_rows.append(dict(x))

        return flask.Response(response=json.dumps(wti06_ETL.count_avg(all_rows)), status=200,
                              mimetype='application/json')

    def get_profile(self, user):
        user_rows = []
        for x in self.client.get_data_table_per_column_value(self.table, 'user_id', user):
            user_rows.append(dict(x))

        all_rows = []
        for x in self.client.get_data_table(self.table):
            all_rows.append(dict(x))

        return flask.Response(response=json.dumps(wti06_ETL.get_profile(all_rows, user_rows)), status=200,
                              mimetype='application/json')

    def add_all_endpoints(self):
        self.app.add_endpoint(endpoint='/rating', endpoint_name='add_rating', handler=self.add_row, methods=['POST', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='delete', handler=self.delete, methods=['DELETE', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/all-users', endpoint_name='get_all_avg_users',
                              handler=self.get_all_avg_users, methods=['GET', ])
        self.app.add_endpoint(endpoint='/profile/<user>', endpoint_name='get_profile_user',
                              handler=self.get_profile, methods=['GET', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/<user>', endpoint_name='get_avg_user',
                              handler=self.get_avg_user, methods=['GET', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='get_all_ratings', handler=self.get_all,
                              methods=['GET', ])
