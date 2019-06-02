import wti01_module
import wti03_ETL
import pandas
import flask
import json

user_ratemovies = pandas.read_csv(filepath_or_buffer='/home/vagrant/user_ratedmovies.dat', sep='\t')
movie_genres = pandas.read_csv(filepath_or_buffer='/home/vagrant/movie_genres.dat', sep='\t')
user_rate_movies_genres = wti03_ETL.build_dataframe(wti01_module.join_tables(user_ratemovies, movie_genres)).head()

my_app = flask.Flask(__name__)


@my_app.route("/ratings", methods=['GET'])
def ratings():
    return flask.Response(response=user_rate_movies_genres.to_json(orient='table', index=False),
                          status=200,
                          mimetype='application/json')


@my_app.route("/rating", methods=['POST'])
def rating():
    global user_rate_movies_genres
    user_rate_movies_genres = user_rate_movies_genres.append(json.loads(flask.request.data), ignore_index=True)
    return flask.Response(response=user_rate_movies_genres.to_json(orient='table', index=False),
                          status=201,
                          mimetype='application/json')


@my_app.route("/ratings", methods=['DELETE'])
def delete_ratings():
    global user_rate_movies_genres
    user_rate_movies_genres = pandas.DataFrame(columns=list(user_rate_movies_genres))
    return flask.Response(response='Deleted', status=204)


@my_app.route('/avg-genre-ratings/all-users', methods=['GET'])
def count_avg_ratings():
    return flask.Response(response=json.dumps(wti03_ETL.count_avg(user_rate_movies_genres)),
                          status=201,
                          mimetype='application/json')


@my_app.route('/avg-genre-ratings/<user>', methods=['GET'])
def count_avg_per_user(user):
    return flask.Response(response=json.dumps(wti03_ETL.count_avg_per_user(user_rate_movies_genres, user)),
                          status=201,
                          mimetype='application/json')


my_app.run(host='0.0.0.0', port='8888')
