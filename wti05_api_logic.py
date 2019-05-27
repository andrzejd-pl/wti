import json

import flask
from flask import Response
from pandas import DataFrame

import wti03_ETL
from wti05_module import FlaskAppWrapper


def count_avg(ratings):
    element = {}
    i = 0

    for row in ratings:
        for col, val in row.items():
            if col in ['movie_id', 'user_id', 'rating', 'user_ratings_id']:
                continue

            if col in element.keys() and val == 1 and element[col] != 'NaN':
                element[col] = float(element[col] + float(row['rating']))
            elif val == 1:
                element[col] = float(row['rating'])
            else:
                element[col] = 'NaN'
        i += 1

    for col, val in element.items():
        if val != 'NaN':
            element[col] = val / i

    return element


class APILogic(object):
    def __init__(self, app=FlaskAppWrapper(name='user_ratings')):
        self.app = app
        self.data_frame = DataFrame(
            columns=['userID', 'movieID', 'rating', 'genre-Animation', 'genre-Musical', 'genre-Drama', 'genre-War',
                     'genre-Romance', 'genre-Sci-Fi', 'genre-Mystery', 'genre-Short', 'genre-Action', 'genre-Western',
                     'genre-Horror', 'genre-Children', 'genre-Comedy', 'genre-Fantasy', 'genre-IMAX', 'genre-Thriller',
                     'genre-Crime', 'genre-Film-Noir', 'genre-Adventure', 'genre-Documentary'])

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
        return flask.Response(response=json.dumps(wti03_ETL.count_avg_per_user(self, user)), status=201,
                              mimetype='application/json')

    def add_all_endpoints(self):
        self.app.add_endpoint(endpoint='/rating', endpoint_name='add_rating', handler=self.add_row, methods=['POST', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='delete', handler=self.delete,
                              methods=['DELETE', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/all-users', endpoint_name='get_all_avg_users',
                              handler=self.get_all_avg_users, methods=['GET', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/<user>', endpoint_name='get_all_avg_users',
                              handler=self.get_all_avg_users, methods=['GET', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='get_all_ratings', handler=self.get_all,
                              methods=['GET', ])
