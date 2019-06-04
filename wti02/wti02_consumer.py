from wti01 import wti01_module
import redis

rds = redis.Redis(host='localhost', port='6381', charset='utf-8', decode_responses=True)

while True:
    wti01_module.receive_message_and_delete(rds)
