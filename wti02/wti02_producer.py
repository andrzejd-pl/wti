import time

from wti01 import wti01_module
import redis

rds = redis.Redis(host='localhost', port='6381', charset='utf-8', decode_responses=True)
rds.flushall()
x = 0
wti01_module.pandas_send_from_file('../data/user_ratedmovies.dat', rds)
exit(0)
while True:
    wti01_module.send_message(rds, 'client', {'message': x})
    x = x + 1
    wti01_module.send_message(rds, 'client', {'message': x})
    x = x + 1
    time.sleep(0.01)
