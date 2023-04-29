package gr.aueb.data_mingler_optimizations.operator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

// Exit error codes:
// 1: wrong number of args
// 2: one of the root-child edges does not exist in Redis
// 3: includeKeys has a value other than "all" or "intersect"

public class thetaCombineOp {
  public static void main(String[] args)  {

	// command arguments should contain:
	// rootNode - string
	// childNode - string rootNode->childNode [this will be the new edge (has to materialize), combining all the output edges]
	// allChildNodes - string, a comma delimited list of all children of rootNode
	// outputChildNodes - string, a comma delimited list of children of rootNode that have an output flag
	// theta - string, a python expression involving rootnode and all or some of the children
	// keysMode - string ("all" or "intersect");


	if (args.length!=6) {
		System.out.println("Wrong Number of arguments");
		System.exit(1); // wrong number of args
	}

	String newNode_name = args[0];
	String includeKeys = args[1];
	String rootNode = args[2];
	int noChildren = args.length - 3;
	List<String> childNodes = new ArrayList<String>();
	for (int i=3; i<args.length; i++)
        childNodes.add(args[i]);

    Jedis jedis = new Jedis("127.0.0.1", 6379);
    Jedis jedis2 = new Jedis("127.0.0.1", 6379);
    Pipeline jPipeline = jedis.pipelined();


    long startTime = System.currentTimeMillis();

	for (String childNode : childNodes) {
	    if (!jedis.exists(rootNode+"-"+childNode)) {
			System.out.println("The edge "+rootNode+"-"+childNode+" does not exist in Redis");
			System.exit(2);
		}
	}

	String newEdge = rootNode + "-" + newNode_name; // new edge for Redis

	Set<String> keys = null;

	// the code below could be very inefficient, union and intersection should be done in Redis but sinter and sunion did not work

	boolean isFirst=true;
	for (String childNode : childNodes) {
		if (isFirst) {
			keys = jedis.smembers(rootNode+"-"+childNode);
			isFirst=false;
			continue;
		}
		if (includeKeys.equals("all"))
			keys.addAll(jedis.smembers(rootNode+"-"+childNode));
		else if (includeKeys.equals("intersect"))
			keys.retainAll(jedis.smembers(rootNode+"-"+childNode));
		else {
			System.out.println("specify whether you want to keep all keys or the intersection of the keys");
			System.exit(3);
		}
	}

	for (String key : keys) {
		//System.out.println(key);
		jPipeline.sadd(newEdge, key); // add the key to the keys' list of new edge
		for (String childNode : childNodes) {
			String edgeToBeAdded = rootNode+"-"+childNode+":"+key; // the key of the list of rootNode->childNode edge
			List<String> values = jedis2.lrange(edgeToBeAdded,0,-1);
			for (String value : values) {
				jPipeline.lpush(newEdge+":"+key,value);
			}
		}
	}
	jPipeline.sync();

	long estimatedTime = System.currentTimeMillis() - startTime;
	System.out.println(estimatedTime);

  } // of main
} // of program


