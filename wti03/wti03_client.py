from wti03 import wti03_ETL
import requests
import json
import threading
import time


def fuction():
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

    r = requests.delete('http://localhost:8888/ratings')
    wti03_ETL.print_request(r)

    r = requests.get('http://localhost:8888/avg-genre-ratings/all-users')
    wti03_ETL.print_request(r)

    r = requests.get('http://localhost:8888/avg-genre-ratings/123')
    wti03_ETL.print_request(r)


threading.Thread(target=fuction).start()
threading.Thread(target=fuction).start()
threading.Thread(target=fuction).start()
threading.Thread(target=fuction).start()
threading.Thread(target=fuction).start()
threading.Thread(target=fuction).start()
threading.Thread(target=fuction).start()
time.sleep(2)
