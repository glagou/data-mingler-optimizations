import redis
import sys

rootNode = sys.argv[1]
childNode = sys.argv[2]
expressionCL = sys.argv[3]
expression = expressionCL.replace("$"+childNode+"$","Lvalue")

# print(rootNode)
# print(childNode)
# print(expression)

# connection information for Redis,replace them with your configuration information.
redis_host = "localhost"
redis_port = 6379
redis_password = ""

# create the Redis Connection object
try:
    r = redis.Redis(host = redis_host, port = redis_port, password = redis_password, db = 0, decode_responses = True)

except Exception as e:
    raise Exception(e)

pipe = r.pipeline()
edge = rootNode+"-"+childNode
keys = r.smembers(edge)
for key in keys:
    values = r.lrange(edge+":"+key,0,-1)
    r.delete(edge+":"+key)
    for Lvalue in values:
        if eval(expression) == True:
            pipe.rpush(edge+":"+key, Lvalue)

pipe.execute()

