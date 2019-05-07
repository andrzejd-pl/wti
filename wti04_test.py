import pandas

import wti04_module

user_ratemovies = pandas.read_csv(filepath_or_buffer='./user_ratedmovies.dat', sep='\t')
movie_genres = pandas.read_csv(filepath_or_buffer='./movie_genres.dat', sep='\t')

join_table, genres = wti04_module.join_tables(user_ratemovies, movie_genres)

# for genre in genres:
#     print(wti04_module.avg_movies_by_genre(join_table, genre))

# for genre in genres:
#     print(wti04_module.avg_movie_by_user_and_genre(join_table, 75, genre))

for genre in genres:
    print(wti04_module.profile_user(join_table, 75, genre))
# join_table, genres = wti04_module.build_dataframe(wtiproj01_module.join_tables(user_ratemovies, movie_genres))
#
# print(wti04_module.calculate_avg_movies_by_genre(join_table, "Documentary"))
