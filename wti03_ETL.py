import pandas


def build_dataframe(user_rates_with_movies):
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

    return pandas.DataFrame.from_dict(elements, orient='index', columns=json_columns_list)


def count_avg_per_user(ratings, user):
    element = {}

    for col in ratings:
        if col in ['movieID', 'userID', 'rating']:
            continue
        element[col] = 0.1
    return element


def count_avg(ratings):
    element = {}

    for col in ratings:
        if col in ['movieID', 'userID', 'rating']:
            continue
        element[col] = 0.2
    return element


def print_request(request):
    print('----------------------------------------------')
    print('request.url: ', request.url)
    print('request.status_code: ', request.status_code)
    print('request.headers: ', request.headers)
    print('request.text: ', request.text)
    print('request.request.headers: ', request.request.headers)
    print('----------------------------------------------')
