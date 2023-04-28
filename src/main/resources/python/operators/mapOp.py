import redis
import sys

rootNode = sys.argv[1]
childNode = sys.argv[2]
importPackage = sys.argv[3]
functionInvocationCL = sys.argv[4]

if importPackage != "":
    importlib.import_module(importPackage)

# print(rootNode)
# print(childNode)
# print(importPackage)
# print(functionInvocation)


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
functionInvocation = functionInvocationCL.replace("$"+childNode+"$","value")
keys = r.smembers(edge)
for key in keys:
    values = r.lrange(edge+":"+key,0,-1)
    r.delete(edge+":"+key)
    for value in values:
        newValue = eval(functionInvocation)
        newValueStr = str(newValue)
        pipe.rpush(edge+":"+key, newValueStr)

pipe.execute()

