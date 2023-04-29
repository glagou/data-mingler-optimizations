package gr.aueb.data_mingler_optimizations.operator;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

// Exit error codes:
// 1: wrong number of args

public class aggregateOp {

  //************** This function returns whether a string is numeric

  public static boolean isNumeric(String strNum) {
      if (strNum == null) {
          return false;
      }
      try {
          double d = Double.parseDouble(strNum);
      } catch (NumberFormatException nfe) {
          return false;
      }
      return true;
  }

  //************** This function returns true if all elements in a list of strings are numeric

  public static boolean listIsNumeric (List<String> list) {
	  boolean result = true;
	  for (String element : list) {
		  result = result && isNumeric(element);
		  if (result==false) break;
	  }
	  return result;
  }

  //**************** MAIN FUNCTION

  public static void main(String[] args) throws IOException {

	// command arguments should contain:
	// rootNode - string
	// childNode - string
	// aggrType - string = ("min/max/average/count/sum/any")

	if (args.length!=3) {
		System.out.println("Wrong Number of arguments");
		System.exit(1); // wrong number of args
	}

	String rootNode = args[0];
	String childNode = args[1];
	String aggrType = args[2];


    Jedis jedis = new Jedis("127.0.0.1", 6379);
    Jedis jedis2 = new Jedis("127.0.0.1", 6379);
    Pipeline jPipeline = jedis2.pipelined();

    long startTime = System.currentTimeMillis();

	String edge = rootNode+"-"+childNode;

	// min case
	if (aggrType.equals("min")) {
		Set<String> keys = jedis.smembers(edge);
		for (String key: keys) {
			String result="";
			List<String> values = jedis.lrange(edge+":"+key,0,-1);
			if (values.size()!=0) {
				jPipeline.del(edge+":"+key);
				if (listIsNumeric(values)) {
					double min = Double.MAX_VALUE;
					for (String value : values) {
						double d = Double.parseDouble(value);
						if (d < min)
							min = d;
					}
					result = String.valueOf(min);
				}
				else
					result=Collections.min(values);
				jPipeline.rpush(edge+":"+key, result);
			}
			else {  // the list of values was empty
				jPipeline.rpush(edge+":"+key, "null");
			}
		}
		jPipeline.sync();
	} // of min case

	// max case
	if (aggrType.equals("max")) {
		Set<String> keys = jedis.smembers(edge);
		for (String key: keys) {
			String result="";
			List<String> values = jedis.lrange(edge+":"+key,0,-1);
			if (values.size()!=0) {
				jPipeline.del(edge+":"+key);
				if (listIsNumeric(values)) {
					double max = Double.MIN_VALUE;
					for (String value : values) {
						double d = Double.parseDouble(value);
						if (d > max)
						  	max = d;
				  	}
				  	result = String.valueOf(max);
				}
				else
				  	result=Collections.max(values);
				jPipeline.rpush(edge+":"+key, result);
			}
			else {  // the list of values was empty
				jPipeline.rpush(edge+":"+key, "null");
			}
		}
		jPipeline.sync();
	} // of max case

	// sum case
	if (aggrType.equals("sum")) {
		Set<String> keys = jedis.smembers(edge);
		for (String key: keys) {
			String result="";
			List<String> values = jedis.lrange(edge+":"+key,0,-1);
			if (values.size()!=0) {
				jPipeline.del(edge+":"+key);
				boolean foundAtLeastOne = false;
				double sum=0;
				for (String value : values) {
					if (!isNumeric(value))
						continue;
					double val = Double.parseDouble(value);
					foundAtLeastOne = true;
					sum=sum+val;
				}
				if (foundAtLeastOne) {
					result = String.valueOf(sum);
					jPipeline.rpush(edge+":"+key, result);
				}
				else { // all values were not 'summable'
					jPipeline.rpush(edge+":"+key, "null");
				}
			}
			else {  // the list of values was empty
				jPipeline.rpush(edge+":"+key, "null");
			}
		}
		jPipeline.sync();
	} // of sum case

	// average case
	if (aggrType.equals("average")) {
		Set<String> keys = jedis.smembers(edge);
		for (String key: keys) {
			String result="";
			List<String> values = jedis.lrange(edge+":"+key,0,-1);
			if (values.size()!=0) {
				jPipeline.del(edge+":"+key);
				boolean foundAtLeastOne = false;
				double sum=0;
				int cnt=0;
				for (String value : values) {
					if (!isNumeric(value))
						continue;
					double val = Double.parseDouble(value);
					foundAtLeastOne = true;
					sum=sum+val;
					cnt++;
				}
				if (foundAtLeastOne) {
					result = String.valueOf(sum/cnt);
					jPipeline.rpush(edge+":"+key, result);
				}
				else { // all values were not 'summable'
					jPipeline.rpush(edge+":"+key, "null");
				}
			}
			else {  // the list of values was empty
				jPipeline.rpush(edge+":"+key, "null");
			}
		}
		jPipeline.sync();
	} // of average


	// count case
	if (aggrType.equals("count")) {
		Set<String> keys = jedis.smembers(edge);
		for (String key: keys) {
			String result="";
			long size = jedis.llen(edge+":"+key);
			jPipeline.del(edge+":"+key);
			result=String.valueOf(size);
			jPipeline.rpush(edge+":"+key, result);
		}
		jPipeline.sync();
	} // of count case

	// any case
	if (aggrType.equals("any")) {
		Set<String> keys = jedis.smembers(edge);
		for (String key: keys) {
			String result="";
			List<String> values = jedis.lrange(edge+":"+key,0,-1);
			if (values.size()!=0) {
				jPipeline.del(edge+":"+key);
				result=values.get(0);
				jPipeline.rpush(edge+":"+key, result);
			}
			// else do nothing, leave it as is
		}
		jPipeline.sync();
	} // of any case

	long estimatedTime = System.currentTimeMillis() - startTime;
	System.out.println(estimatedTime);

  } // of main
} // of program
