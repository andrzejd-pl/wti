import json
import time
import pandas


def send_message(redis_connection, channel, message):
    redis_connection.rpush(channel, json.dumps(message))


def receive_message_and_delete(redis_connection, channel):
    try:
        messages = redis_connection.lrange(channel, 0, -1)
        for message_json in messages:
            message = json.loads(message_json)
            print(message)

        redis_connection.ltrim(channel, len(messages), -1)
    except IndexError:
        time.sleep(0.01)
        return receive_message_and_delete(redis_connection, channel)


def pandas_send_from_file(filename, redis_connection, channel):
    dataset = pandas.read_csv(filepath_or_buffer=filename, sep='\t')
    for row in dataset.to_dict(orient="records"):
        send_message(redis_connection, channel, json.dumps(row))
        time.sleep(0.6)


def join_tables(user_ratedmovies, movie_genres):
    indexes = movie_genres.groupby(by='movieID').apply(lambda x: list(x.genre)).to_frame()
    indexes.columns = ['genres']
    return user_ratedmovies.join(indexes, on='movieID')
