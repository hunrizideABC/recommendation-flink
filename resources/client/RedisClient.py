import redis
class RedisClient:
    def __init__(self):
        self.jedis = redis.StrictRedis(
            host='43.130.37.56',
            port=6379,
            db=0
        )

    def getData(self, key):
        return self.jedis.get(key)

    def setData(self, key, value):
        return self.jedis.set(key, value)

    def getTopList(self, top_range):
        res = []
        for i in range(top_range):
            res.append(self.getData(str(i)))
        return res

if __name__ == "__main__":
    redis_client = RedisClient()
    print(redis_client.getData("1"))
    print(redis_client.getTopList(2))