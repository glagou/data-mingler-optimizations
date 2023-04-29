import redis
import sys

# rootNode - string
# childNode - string rootNode->childNode [this will be the new edge (has to materialize), combining all the output edges]
# allChildNodes - string, a comma delimited list of all children of rootNode
# outputChildNodes - string, a comma delimited list of children of rootNode that have an output flag
# theta - string, a python expression involving rootnode and all or some of the children
# keysMode - string ("all" or "intersect");

rootNode = sys.argv[1]
newChildNode = sys.argv[2]
allChildNodesCL = sys.argv[3]
outputChildNodesCL = sys.argv[4]
thetaCL = sys.argv[5]
keysMode = sys.argv[6]



# all internal nodes should have some output child - FOR NOW. 
# You can have a tree only for a theta expression (i.e. no output children), but it has not been implemented YET
# UPDATE 29/3/2020: implemented for all cases
# if there is no outputChildNodes, i.e. the tree represent only a theta condition (which happens very often)
# then you construct the new edge (childNode) as follows: 
# for each key that satisfies the condition, insert in key's list the key itself as the only value!
# We do this is because right after the thetaCombine gr.aueb.data_mingler_optimizations.operator you are going to have a rollup gr.aueb.data_mingler_optimizations.operator
# If the internal node represents just a theta expression, you want to replace the key of edge2 with itself, so the edge
# that results from the rollup gr.aueb.data_mingler_optimizations.operator is edge1 with just fewer keys (i.e. applying the theta epression)
    
if (outputChildNodesCL == ""):
    hasOutput=False
else:
	hasOutput=True

allChildNodes = allChildNodesCL.split(',')
outputChildNodes = outputChildNodesCL.split(',')

theta = "True"
if (thetaCL!=""):	
	theta = thetaCL.replace("$"+rootNode+"$","key")
	for childNode in allChildNodes:
		theta = theta.replace("$"+childNode+"$","r.lrange(\""+rootNode+"-"+childNode+":\"+key,0,1)[0]");


# connection information for Redis,replace them with your configuration information.
redis_host = "localhost"
redis_port = 6379
redis_password = ""

# create the Redis Connection object
try:
    r = redis.Redis(host = redis_host, port = redis_port, password = redis_password, db = 0, decode_responses = True)

except Exception as e:
    raise Exception(e)
	
print(allChildNodes)
print(outputChildNodes)
print(theta)
print(hasOutput)
	
pipe = r.pipeline()

# in this version keysMode is completely ignored - and most likely there is no reason to have it:
# keysMode pertains only to output nodes, however the common case is to have only one output node
# keys of all nodes mentioned in theta condition should be intersected because you may have nodes that are roots
# for trees representing only thetas, i.e. no output node - in that case, an intersection leaves the correct keys
# So it should be keysMode(outputNodes) intersect (intersection(theta nodes)) - however a theta node could exist in the output nodes

isFirst=True
for childNode in allChildNodes: 
	if (isFirst):
		keys = r.smembers(rootNode+"-"+childNode)
		isFirst=False
		continue;
	# if (keysMode=="all"):
	#	keys=keys.union(r.smembers(rootNode+"-"+childNode));
	# else:
	keys=keys.intersection(r.smembers(rootNode+"-"+childNode));


newEdge = rootNode+"-"+newChildNode

for key in keys:
	if eval(theta)==True:
		pipe.sadd(newEdge, key) # add the key to the keys' list of new edge
		if (hasOutput):
			for childNode in outputChildNodes:
				nextEdge = rootNode+"-"+childNode+":"+key
				values = r.lrange(nextEdge,0,-1)
				for value in values:
					pipe.rpush(newEdge+":"+key,value)
		else:
			pipe.rpush(newEdge+":"+key,key)
		
pipe.execute()

