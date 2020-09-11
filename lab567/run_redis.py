import redis

myHostname = "redis-iot-lab1.redis.cache.windows.net"
myPassword = "enL88Gh80HBde7Od2kNj3HWYWBPlE0mnRt0KNGxgrmw="

r = redis.StrictRedis(host=myHostname, port=6380,
                      password=myPassword, ssl=True)

result = r.ping()
print("Ping returned : " + str(result))