import json

import requests

from wti03 import wti03_ETL


def main():
    r = requests.delete('http://localhost:8888/ratings')
    wti03_ETL.print_request(r)

    r = requests.get('http://localhost:8888/ratings')
    wti03_ETL.print_request(r)

    r = requests.post('http://localhost:8888/rating', json.dumps({
        "user_id": 75,
        "movie_id": 110,
        "rating": 4,
        "genre_romance": 0,
        "genre_drama": 1,
        "genre_mystery": 0,
        "genre_sci_fi": 0,
        "genre_short": 0,
        "genre_war": 1,
        "genre_animation": 0,
        "genre_musical": 0,
        "genre_documentary": 0,
        "genre_crime": 0,
        "genre_children": 0,
        "genre_thriller": 0,
        "genre_horror": 0,
        "genre_film_noir": 0,
        "genre_western": 0,
        "genre_imax": 0,
        "genre_adventure": 0,
        "genre_fantasy": 0,
        "genre_action": 1,
        "genre_comedy": 0
    }))
    wti03_ETL.print_request(r)

    r = requests.post('http://localhost:8888/rating', json.dumps({
        "user_id": 75,
        "movie_id": 112,
        "rating": 5,
        "genre_romance": 0,
        "genre_drama": 1,
        "genre_mystery": 0,
        "genre_sci_fi": 0,
        "genre_short": 0,
        "genre_war": 1,
        "genre_animation": 0,
        "genre_musical": 0,
        "genre_documentary": 0,
        "genre_crime": 0,
        "genre_children": 0,
        "genre_thriller": 0,
        "genre_horror": 0,
        "genre_film_noir": 0,
        "genre_western": 0,
        "genre_imax": 0,
        "genre_adventure": 0,
        "genre_fantasy": 0,
        "genre_action": 1,
        "genre_comedy": 0
    }))
    wti03_ETL.print_request(r)

    r = requests.post('http://localhost:8888/rating', json.dumps({
        "user_id": 73,
        "movie_id": 110,
        "rating": 6,
        "genre_romance": 0,
        "genre_drama": 1,
        "genre_mystery": 0,
        "genre_sci_fi": 0,
        "genre_short": 0,
        "genre_war": 1,
        "genre_animation": 0,
        "genre_musical": 0,
        "genre_documentary": 0,
        "genre_crime": 0,
        "genre_children": 0,
        "genre_thriller": 0,
        "genre_horror": 0,
        "genre_film_noir": 0,
        "genre_western": 0,
        "genre_imax": 0,
        "genre_adventure": 0,
        "genre_fantasy": 0,
        "genre_action": 1,
        "genre_comedy": 0
    }))
    wti03_ETL.print_request(r)

    r = requests.post('http://localhost:8888/rating', json.dumps({
        "user_id": 73,
        "movie_id": 113,
        "rating": 1,
        "genre_romance": 0,
        "genre_drama": 1,
        "genre_mystery": 0,
        "genre_sci_fi": 0,
        "genre_short": 0,
        "genre_war": 1,
        "genre_animation": 0,
        "genre_musical": 0,
        "genre_documentary": 0,
        "genre_crime": 0,
        "genre_children": 0,
        "genre_thriller": 0,
        "genre_horror": 0,
        "genre_film_noir": 0,
        "genre_western": 0,
        "genre_imax": 0,
        "genre_adventure": 0,
        "genre_fantasy": 0,
        "genre_action": 1,
        "genre_comedy": 0
    }))
    wti03_ETL.print_request(r)

    r = requests.get('http://localhost:8888/avg-genre-ratings/all-users')
    wti03_ETL.print_request(r)

    r = requests.get('http://localhost:8888/avg-genre-ratings/75')
    wti03_ETL.print_request(r)

    r = requests.get('http://localhost:8888/profile/75')
    wti03_ETL.print_request(r)

    r = requests.delete('http://localhost:8888/ratings')
    wti03_ETL.print_request(r)


if __name__ == '__main__':
    main()
