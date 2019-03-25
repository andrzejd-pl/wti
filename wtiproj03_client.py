import wti03_ETL
import requests
import json
import threading
import time


def fuction():
    r = requests.get('http://localhost:8888/ratings')
    wti03_ETL.print_request(r)

    r = requests.post('http://localhost:8888/rating', json.dumps({
        "userID": 75,
        "movieID": 110,
        "rating": 4,
        "genre-Romance": 0,
        "genre-Drama": 1,
        "genre-Mystery": 0,
        "genre-Sci-Fi": 0,
        "genre-Short": 0,
        "genre-War": 1,
        "genre-Animation": 0,
        "genre-Musical": 0,
        "genre-Documentary": 0,
        "genre-Crime": 0,
        "genre-Children": 0,
        "genre-Thriller": 0,
        "genre-Horror": 0,
        "genre-Film-Noir": 0,
        "genre-Western": 0,
        "genre-IMAX": 0,
        "genre-Adventure": 0,
        "genre-Fantasy": 0,
        "genre-Action": 1,
        "genre-Comedy": 0
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
