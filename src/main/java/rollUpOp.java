import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.List;
import java.util.Set;

// Exit error codes:
// 1: wrong number of args

public class rollUpOp {

  public static void main(String[] args) throws IOException {

	// command arguments should contain:
	// rootNode - string
	// childNode - string
	// childChildNode - string

	if (args.length!=3) {
		System.out.println("Wrong Number of arguments");
		System.exit(1); // wrong number of args
	}

	String rootNode = args[0];
	String childNode = args[1];
	String childChildNode = args[2];

    Jedis jedis = new Jedis("127.0.0.1", 6379);
    Jedis jedis2 = new Jedis("127.0.0.1", 6379);
    Pipeline jPipeline = jedis2.pipelined();

    long startTime = System.currentTimeMillis();

	String edge1 = rootNode+"-"+childNode;
	String edge2 = childNode + "-" + childChildNode;
	Set<String> keys = jedis.smembers(edge1);
	for (String key: keys) {
		List<String> values = jedis.lrange(edge1+":"+key,0,-1);
		jPipeline.del(edge1+":"+key);
		for (String value : values) {
			List<String> values2 = jedis.lrange(edge2+":"+value,0,-1);
			for (String value2: values2) {
			  	jPipeline.rpush(edge1+":"+key,value2);
			}
		}
	}
	jPipeline.sync();

	long estimatedTime = System.currentTimeMillis() - startTime;
	System.out.println(estimatedTime);

  } // of main
} // of program
