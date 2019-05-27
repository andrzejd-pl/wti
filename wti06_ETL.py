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
