import json

import flask
from flask import Response
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

import wti06_ETL
from wti05_module import FlaskAppWrapper
from wti06_cassandra_client import create_keyspace, create_table, push_data_table, get_data_table, clear_table, get_data_table_per_column_value


class APILogic(object):
    def __init__(self, app=FlaskAppWrapper(name='tmp2')):
        self.app = app
        self.keyspace = "user_ratings"
        self.table = "user_avg_rating"
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        create_keyspace(self.session, self.keyspace)
        self.session.set_keyspace(self.keyspace)
        self.session.row_factory = dict_factory
        create_table(self.session, self.keyspace, self.table)
        self.next_index = 0

    def add_row(self):
        data = json.loads(flask.request.data)
        data['user_ratings_id'] = self.next_index
        self.next_index += 1
        push_data_table(self.session, self.keyspace, self.table, data)
        return Response(status=201)

    def get_all(self):
        all_rows = []
        for x in get_data_table(self.session, self.keyspace, self.table):
            all_rows.append(dict(x))

        return Response(status=200, response=json.dumps(all_rows),
                        mimetype='application/json')

    def delete(self):
        clear_table(self.session, self.keyspace, self.table)

        return Response(status=204)

    def get_all_avg_users(self):
        all_rows = []
        for x in get_data_table(self.session, self.keyspace, self.table):
            all_rows.append(dict(x))

        return flask.Response(response=json.dumps(wti06_ETL.count_avg(all_rows)), status=201,
                              mimetype='application/json')

    def get_avg_user(self, user):
        all_rows = []
        for x in get_data_table_per_column_value(self.session, self.keyspace, self.table, 'user_id', user):
            all_rows.append(dict(x))

        return flask.Response(response=json.dumps(wti06_ETL.count_avg(all_rows)), status=201,
                              mimetype='application/json')

    def add_all_endpoints(self):
        self.app.add_endpoint(endpoint='/rating', endpoint_name='add_rating', handler=self.add_row, methods=['POST', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='delete', handler=self.delete, methods=['DELETE', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/all-users', endpoint_name='get_all_avg_users',
                              handler=self.get_all_avg_users, methods=['GET', ])
        self.app.add_endpoint(endpoint='/avg-genre-ratings/<user>', endpoint_name='get_avg_user',
                              handler=self.get_avg_user, methods=['GET', ])
        self.app.add_endpoint(endpoint='/ratings', endpoint_name='get_all_ratings', handler=self.get_all,
                              methods=['GET', ])
