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
            element[col] = val/i

    return element


def get_profile(ratings, user_ratings):
    avg_all = count_avg(ratings)
    avg_user = count_avg(user_ratings)
    profile = {}

    for column in avg_user:
        if avg_user[column] == 'NaN':
            profile[column] = 0
        else:
            profile[column] = avg_all[column] - avg_user[column]

    return profile
