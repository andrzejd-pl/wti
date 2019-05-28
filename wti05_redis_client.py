import json
import time
import numpy as np

import redis
from pandas import DataFrame


class RedisClient(object):
    def __init__(self, host='localhost', port='6381', charset='utf-8', decode_responses=True):
        self.redis_connection = redis.Redis(host=host, port=port, charset=charset, decode_responses=decode_responses)

    def send_message(self, channel, message):
        self.redis_connection.rpush(channel, json.dumps(message))

    def receive_message(self, channel, array=None):
        if array is None:
            array = []
        try:
            for el in self.redis_connection.lrange(channel, 0, -1):
                value = json.loads(el)
                array.append(value)
            return array
        except IndexError:
            time.sleep(0.01)
            return self.receive_message(channel, array)

    @staticmethod
    def count_avg_in_df(data_frame: DataFrame, columns):
        element = {}

        for column in columns:
            if column in ['user_id', 'movie_id', 'rating']:
                continue

            avg = data_frame.loc[data_frame[column] == 1][['rating']].mean().values[0]
            if np.isnan(avg):
                element[column] = 'NaN'
            else:
                element[column] = avg

        return element

    def count_avg(self, ratings):
        columns = []

        for column in ratings[0]:
            columns.append(column)

        return self.count_avg_in_df(DataFrame(ratings), columns)

    def get_user_profile(self, channel, user_id: int) -> dict:
        user_ratings_df = self.select_user(DataFrame(self.receive_message(channel)), user_id)
        avg_all = self.count_avg_all(channel)
        avg_user = self.count_avg_in_df(user_ratings_df, list(user_ratings_df.columns))
        profile = {}

        for column in avg_user:
            if avg_user[column] == 'NaN':
                profile[column] = 0
            else:
                profile[column] = avg_all[column] - avg_user[column]

        return profile

    @staticmethod
    def select_user(ratings: DataFrame, user_id: int) -> DataFrame:
        return ratings.loc[ratings['user_id'] == int(user_id)]

    def count_avg_all(self, channel):
        return self.count_avg(self.receive_message(channel))

    def delete_all(self):
        self.redis_connection.flushall()
