import json

import flask
import numpy
from flask import Response
from pandas import DataFrame

from wti05_module import FlaskAppWrapper
from wti05_redis_client import RedisClient


def count_avg(ratings: DataFrame):
    element = {}
    columns = list(ratings.columns)

    for column in columns:
        if column in ['user_id', 'movie_id', 'rating']:
            continue

        avg = ratings.loc[ratings[column] == 1][['rating']].mean().values[0]
        if numpy.isnan(avg):
            element[column] = 'NaN'
        else:
            element[column] = avg

    return element


def select_user(ratings: DataFrame, id: int) -> DataFrame:
    return ratings.loc[ratings['user_id'] == int(id)]


def get_user_profile(ratings: DataFrame, user_id: int) -> dict:
    avg_all = count_avg(ratings)
    avg_user = count_avg(select_user(ratings, int(user_id)))
    profile = {}

    for column in avg_user:
        if avg_user[column] == 'NaN':
            profile[column] = 0
        else:
            profile[column] = avg_all[column] - avg_user[column]

    return profile


class APILogicDF(object):
    def __init__(self, app=FlaskAppWrapper(name='user_ratings')):
        self.app = app
        self.data_frame = DataFrame(
            columns=['user_id', 'movie_id', 'rating', 'genre_animation', 'genre_musical', 'genre_drama', 'genre_war',
                     'genre_romance', 'genre_sci_fi', 'genre_mystery', 'genre_short', 'genre_action', 'genre_western',
                     'genre_horror', 'genre_children', 'genre_comedy', 'genre_fantasy', 'genre_imax', 'genre_thriller',
                     'genre_crime', 'genre_film_noir', 'genre_adventure', 'genre_documentary'])

    def add_row(self):
        self.data_frame = self.data_frame.append(json.loads(flask.request.data), ignore_index=True)
        return Response(status=201)

    def get_all(self):
        return Response(status=200, response=self.data_frame.to_json(orient='table', index=False),
                        mimetype='application/json')

    def delete(self):
        self.data_frame = DataFrame(columns=list(self.data_frame.columns))
        return Response(status=204)

    def get_all_avg_users(self):
        return flask.Response(response=json.dumps(count_avg(self.data_frame)), status=201,
                              mimetype='application/json')

    def get_avg_user(self, user):
        return flask.Response(response=json.dumps(get_user_profile(self.data_frame, user)), status=201,
                              mimetype='application/json')

    def add_all_endpoints(self):
        self.app.add_endpoint(endpoint='/rating', endpoint_name='add_rating', handler=self.add_row, methods=['POST', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='delete', handler=self.delete,
                              methods=['DELETE', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/all-users', endpoint_name='get_all_avg_users',
                              handler=self.get_all_avg_users, methods=['GET', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/<user>', endpoint_name='get_avg_user',
                              handler=self.get_avg_user, methods=['GET', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='get_all_ratings', handler=self.get_all,
                              methods=['GET', ])


class APILogic(object):
    def __init__(self, app=FlaskAppWrapper(name='user_ratings')):
        self.app = app
        self.redis = RedisClient()
        self.channel = 'api'

    def add_row(self):
        self.redis.send_message(self.channel, json.loads(flask.request.data))
        return Response(status=201)

    def get_all(self):
        return Response(status=200, response=json.dumps(self.redis.receive_message(self.channel)),
                        mimetype='application/json')

    def delete(self):
        self.redis.delete_all()
        return Response(status=204)

    def get_all_avg_users(self):
        return flask.Response(response=json.dumps(self.redis.count_avg_all(self.channel)), status=201,
                              mimetype='application/json')

    def get_avg_user(self, user):
        return flask.Response(response=json.dumps(self.redis.get_user_profile(channel=self.channel, user_id=user)),
                              status=201, mimetype='application/json')

    def add_all_endpoints(self):
        self.app.add_endpoint(endpoint='/rating', endpoint_name='add_rating', handler=self.add_row, methods=['POST', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='delete', handler=self.delete,
                              methods=['DELETE', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/all-users', endpoint_name='get_all_avg_users',
                              handler=self.get_all_avg_users, methods=['GET', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/<user>', endpoint_name='get_avg_user',
                              handler=self.get_avg_user, methods=['GET', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='get_all_ratings', handler=self.get_all,
                              methods=['GET', ])
