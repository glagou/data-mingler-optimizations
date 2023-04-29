package gr.aueb.data_mingler_optimizations.operator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.List;
import java.util.Set;

// Exit error codes:
// 1: wrong number of args

public class mapOp {

  public static void main(String[] args) throws IOException {

	// command arguments should contain:
	// rootNode - string
	// childNode - string
	// functionName - string

	if (args.length!=3) {
		System.out.println("Wrong Number of arguments");
		System.exit(1); // wrong number of args
	}

	String rootNode = args[0];
	String childNode = args[1];
	String functionName = args[2];

    Jedis jedis = new Jedis("127.0.0.1", 6379);
    Jedis jedis2 = new Jedis("127.0.0.1", 6379);
    Pipeline jPipeline = jedis2.pipelined();

    long startTime = System.currentTimeMillis();

	String edge = rootNode+"-"+childNode;
	Set<String> keys = jedis.smembers(edge);
	for (String key: keys) {
		List<String> values = jedis.lrange(edge+":"+key,0,-1);
		jPipeline.del(edge+":"+key);
		for (String value : values) {
			jPipeline.rpush(edge+":"+key,String.valueOf(functionName(value)));
		}
	}
	jPipeline.sync();

	long estimatedTime = System.currentTimeMillis() - startTime;
	System.out.println(estimatedTime);

  } // of main
} // of program
