import pandas
import numpy
import wtiproj01_module


def join_tables(user_ratedmovies, movie_genres):
    user_rates_with_movies = wtiproj01_module.join_tables(user_ratedmovies, movie_genres)
    genres_list = list(set([item for sublist in user_rates_with_movies['genres'].tolist() for item in sublist]))
    json_columns_list = list(['userID', 'movieID', 'rating'])
    json_columns_list.extend(list(('genre-' + item) for item in genres_list))
    elements = {}
    iterator = 0

    for row in user_rates_with_movies[['userID', 'movieID', 'rating', 'genres']].to_dict(orient='records'):
        element = [row['userID'], row['movieID'], row['rating']]

        for genre in genres_list:
            element.extend([int((genre in row['genres']) if 1 else 0)])

        elements[iterator] = element
        iterator = iterator + 1

    return pandas.DataFrame.from_dict(elements, orient='index', columns=json_columns_list), genres_list


def build_dataframe_from_dict(elements, columns):
    return pandas.DataFrame.from_dict(elements, orient='index', columns=columns)


def build_dict_from_dataframe(dataframe):
    return dataframe.to_dict(orient='records')


def avg_movies_by_genre(dataframe, genre):
    avg = pandas.pivot_table(dataframe, values='rating', aggfunc=numpy.average, index=['genre-' + genre])
    avg = avg.drop(0)

    if avg.empty:
        return {'genre': genre, 'rating': False}
    else:
        return {'genre': genre, 'rating': avg.iloc[0].rating}


def avg_movie_by_user_and_genre(dataframe, user_id, genre):
    return {
        'user_id': user_id,
        'genre': genre,
        'rating': avg_movies_by_genre(dataframe.loc[lambda df: df.userID == user_id, :], genre)['rating']
    }


def profile_user(dataframe, user_id, genres):
    ratings = []
    for genre in genres:
        avg_all = avg_movies_by_genre(dataframe, genre)
        avg_genre = avg_movie_by_user_and_genre(dataframe, user_id, genre)
        ratings.extend([{
            'genre': genre,
            'rating': ((avg_genre['rating'] - avg_all['rating']) if avg_genre['rating'] else 0)
        }])

    return {
        'user_id': user_id,
        'ratings': ratings
    }
