import org.neo4j.driver.v1.*;
import org.w3c.dom.Document;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Evaluates a query (Q) defined over a DVM (Data Virtual Machine).
 * <br>
 * The query (Q) is a tree having as a root the node R, described in the corresponding query XML file that has been
 * previously generated.
 * <br>
 * The transformations currently supported are: aggregate, filter, and map.
 * One can have a sequence of transformations in a ":" seperated list:
 * e.g. aggregate:avg
 * <br>
 * The error codes that the program currently exits with indicate the following:
 * 1: wrong number of arguments given
 * 2: second argument is not 'excel' or 'none'
 * 3: third argument is not 'all' or 'intersect'
 * 4: loading of edges has failed
 * 5: execution of an operator has failed
 */
public class evalQuery {

	// TODO: parallel evals
	// TODO: deallocate Redis space (remove KL structures) as soon as possible
	// TODO: each edge consists of keys in the form "root-child:key" - could be a tremendous waste of memory space;
	//  maybe each edge can get an id (int)

  // This function gets a rootNode and a subset of its children and returns the *union* of the keys of all the involved edges rootNode --> childNode
  // The code below could be quite inefficient, union should be done in Redis but sunion did not work

  static org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic("neo4j", "1234"));
  static Jedis jedis = new Jedis("127.0.0.1", 6379);
  static Jedis jedis2 = new Jedis("127.0.0.1", 6379);
  static Pipeline jPipeline = jedis2.pipelined();

  static Map<String, List<String>> childrenLists = new HashMap<String, List<String>>(); // the children's list of each node in the query (global)
  static Map<String, String> onNodes = new HashMap<String, String>(); // the actual node in the DVM that a label corresponds to (e.g. X --> custID)
  static Map<String, String> transformations = new HashMap<String, String>(); // the transformations' sequence of each node in the query
  static Map<String, String> thetas = new HashMap<String, String>(); // the theta expression that has to be expressed on internal nodes in the query
  static Map<String, String> outputs = new HashMap<String, String>(); // "yes" or "no" - should the computed edge participate in the output or will it be used just in the theta?

  static List<String> arguments = new ArrayList<String>(); // list contaning the args required for loadEdges command line arguments
  static String path2Python = "C:\\Users\\Damianos\\AppData\\Local\\Programs\\Python\\Python37\\";


  //************** This functions deletes the rootNode -> childNode edge from Redis
  public static void removeEdge (String rootNode, String childNode) {
	  Set<String> keys = jedis.smembers(rootNode+"-"+childNode);
	  for (String key : keys)
	  	  jPipeline.del(rootNode+"-"+childNode+":"+key);
	  jPipeline.del(rootNode+"-"+childNode);
	  jPipeline.sync();
  }


  //************** This functions selects keys of a rootNode based on its children
  public static Set<String> combineKeys (String rootNode, List<String> childNodes, String keysMode) {
	  Set<String> keys = null;
	  boolean isFirst = true;
	  for (String childNode : childNodes) {
		  if (isFirst) {
			  keys = jedis.smembers(rootNode+"-"+childNode);
			  isFirst=false;
			  continue;
		  }
		  if (keysMode.equals("all"))
  			  keys.addAll(jedis.smembers(rootNode+"-"+childNode));
	  	  else if (keysMode.equals("intersect"))
	  	  	  keys.retainAll(jedis.smembers(rootNode+"-"+childNode));
	  }
	  return keys;
  } // of combineKeys


  //************** This function executes a transformation on an edge (aggregation, filtering, etc.) and MODIFIES the edge
  public static void execTransformations (String rootNode, String childNode) throws IOException {
	  String transforms = "";
	  int numOfTransformations = 0;

      // older versions where poperties of the query were stored in the DVM graph in Neo4j
      //try (Session session = driver.session()) {
		//  StatementResult result = session.run("MATCH (n:attribute{name:'"+rootNode+"'})-[r:has]->(m:attribute{name:'"+childNode+"'}) RETURN r.transformations AS name");
        //  while (result.hasNext()) {
		//	  Record record = result.next();
        //    transformations = record.get("name").asString();
        //  }
      //}

      transforms = transformations.get(childNode);
      String[] transformation = transforms.split(";",-1);
      if (!transforms.equals("null") && !transforms.trim().equals("")) {
		  numOfTransformations = transformation.length;
	  }
	  for (int i=0; i<numOfTransformations; i++) {
		  String[] args = transformation[i].split(":",-1);
		  String nameOfOper = args[0];
		  String parameters = args[1];
		  System.out.println("  **  Operator:"+nameOfOper+", params:"+parameters); // debug
		  String[] parameter = parameters.split(",",-1);

		  // aggregation - one parameter, the type of aggregation (min, max, average, count, sum, any)
		  if (nameOfOper.equals("aggregate")) {  // calling aggregation operator

			  /* using class call
			  String[] cmdArgs = {rootNode, childNode, parameter[0]};
			  try {
				  aggregateOp.main(cmdArgs);
			  }
			  catch (Exception e) {
				  System.out.println("aggregateOp failed for rootNode: "+rootNode+" and childNode: "+childNode+" and function: "+parameter[0]);
				  System.exit(5);
			  }
			  */

      		  // using runtime
		      try {
				  Process p = Runtime.getRuntime().exec("java aggregateOp "+rootNode+" "+childNode+" "+parameter[0]);
				  p.waitFor();
	      	      int returnValue = p.exitValue();
	      	      if (returnValue!=0) {
					  System.out.println("aggregateOp failed for rootNode: "+rootNode+" and childNode: "+childNode+" and function: "+parameter[0]+" with error code: "+returnValue);
					  System.exit(5);
				  }

		  	  }
		  	  catch (InterruptedException e) {
				  e.printStackTrace();
		  	  }
		  	  //

		  } // of aggregation

		  // filtering
		  if (nameOfOper.equals("filter")) {  // calling filtering operator - written in python!
		      try {
				  //System.out.println("python filterOp.py "+rootNode+" "+childNode+" \""+parameter[0]+"\"");
				  Process p = Runtime.getRuntime().exec(path2Python+"python filterOp.py "+rootNode+" "+childNode+" \""+parameter[0]+"\"");
				  p.waitFor();
	      	      int returnValue = p.exitValue();
	      	      if (returnValue!=0) {
					  System.out.println("filterOp failed for rootNode: "+rootNode+" and childNode: "+childNode+" and expression: "+parameter[0]+" with error code: "+returnValue);
					  System.exit(5);
				  }
		  	  }
		  	  catch (InterruptedException e) {
				  e.printStackTrace();
		  	  }
		  } // of filtering

		  // mapping - two parameters, the language of the function and the name of the function
		  if (nameOfOper.equals("map")) {  // calling mapping operator
			  String hostPL = parameter[0];
			  String importPackage = parameter[1];
			  String functionName = parameter[2];

			  if (hostPL.equals("java")) { // does not work
				  try {
					  Process p = Runtime.getRuntime().exec("java mapOp "+rootNode+" "+childNode+" "+functionName);
					  p.waitFor();
		      	      int returnValue = p.exitValue();
		      	      if (returnValue!=0) {
						  System.out.println("mapOp failed for rootNode: "+rootNode+" and childNode: "+childNode+" and function: "+functionName+" with error code: "+returnValue);
						  System.exit(5);
					  }
			  	  }
			  	  catch (InterruptedException e) {
					  e.printStackTrace();
			  	  }
			  }

			  if (hostPL.equals("python")) {
			      try {
					  //System.out.println("python mapOp.py "+rootNode+" "+childNode+" \""+importPackage+"\" \""+functionName+"\"");
					  Process p = Runtime.getRuntime().exec(path2Python+"python mapOp.py "+rootNode+" "+childNode+" \""+importPackage+"\" \""+functionName+"\"");
					  p.waitFor();
		      	      int returnValue = p.exitValue();
		      	      if (returnValue!=0) {
						  System.out.println("mapOp failed for rootNode: "+rootNode+" and childNode: "+childNode+" and host language: "+parameter[0]+" and import package: "+parameter[1]+" and function: "+parameter[2]+" with error code: "+returnValue);
						  System.exit(5);
					  }
			  	  }
			  	  catch (InterruptedException e) {
					  e.printStackTrace();
			  	  }
			  }

			  if (hostPL.equals("R")) {
				  // mapOp operator implemented in R should be invoked here
			  }

		  } // of mapping

	  }
  } // of execTransformations


  //************** This function gets a root and its children and combine them into one (new) edge rootNode --> childNode based on a selection expression theta
  public static void thetaCombine(String rootNode, String childNode, String allChildNodes, String outputChildNodes, String theta, String keysMode) throws IOException {

	long starting = System.currentTimeMillis();
	System.out.print("  **  Operator: thetaCombine on:"+rootNode+"("+allChildNodes+") - Elapsed time:" ); // debug
	try {

		ProcessBuilder pb = new ProcessBuilder(path2Python+"python","thetaCombineOp.py",""+rootNode,""+childNode,"\""+allChildNodes+"\"","\""+outputChildNodes+"\"","\""+theta+"\"","\""+keysMode+"\"");
		Process p = pb.start();
		// the code bellow is used for debugging python
		// BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        // String inputLine=null;
        // while ((inputLine = reader.readLine()) != null) {
        //     System.out.println("      * Python Output: "+inputLine);
        // }
        int returnValue = p.waitFor();
	  	if (returnValue!=0) {
			System.out.println("thetaCombineOp failed for rootNode: "+rootNode+", childNode: "+childNode+" and children: "+allChildNodes+" with error code: "+returnValue);
	  		System.exit(5);
	  	}

		//Process p = Runtime.getRuntime().exec("python c:\\datamingler\\implementation\\thetaCombineOp.py "+rootNode+" "+childNode+" \""+allChildNodes+"\" \""+outputChildNodes+"\" \""+theta+"\" \""+keysMode+"\"");

		/* debugging - what python returns
 		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
		BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

		String inputLine = null;
		while ((inputLine = stdInput.readLine()) != null) {
			System.out.println("Line: "+inputLine);
		}

		System.out.println("Here is the standard error of the command:\n");
		String inputLine2 = null;
		while ((inputLine2 = stdError.readLine()) != null) {
			System.out.println("Error: "+inputLine2);
		}
		*/

	  	//p.waitFor();
	  	//int returnValue = p.exitValue();
	  	//if (returnValue!=0) {
		//	System.out.println("thetaCombineOp failed for rootNode: "+rootNode+", childNode: "+childNode+" and children: "+allChildNodes+" with error code: "+returnValue);
	  	//	System.exit(5);
	  	//}

	}
	catch (Exception e) {
		System.out.println("Error in Process Builder");
	  	e.printStackTrace();
	}

	long elapsedTime = System.currentTimeMillis() - starting;
	System.out.println(elapsedTime);

	//catch (InterruptedException e) {
	//  	e.printStackTrace();
	//}

  } // of thetaCombine


  //************** This function gets two edges in a path and joins them together (a rollup)
  public static void rollupEdges(String rootNode, String childNode, String childChildNode) throws IOException {
	System.out.print("  **  Operator: rollUpJoin on:"+rootNode+"->"+childNode+" and "+childNode+"->"+childChildNode+" - Time elapsed: "); // debug

	String[] cmdArgs = {rootNode, childNode, childChildNode};
	try {
		rollUpOp.main(cmdArgs);
	}
	catch (Exception e) {
		System.out.println("rollUpOp failed for rootNode: "+rootNode+", childNode: "+childNode+" and childChildNode: "+childChildNode);
		System.exit(5);
	}


	/*
	try {
		Process p = Runtime.getRuntime().exec("java rollUpOp "+rootNode+" "+childNode+" "+childChildNode);
		p.waitFor();
	    int returnValue = p.exitValue();
	    if (returnValue!=0) {
			System.out.println("rollUpOp failed for rootNode: "+rootNode+", childNode: "+childNode+" and childChildNode: "+childChildNode+" with error code: "+returnValue);
			System.exit(5);
		}
	}
	catch (InterruptedException e) {
		e.printStackTrace();
	}
	*/

  } // of rollUpEdges


  //************** This function recursively evals the child of a node
  public static void evalChild(String rootNode, String childNode, String keysMode) throws IOException {
	  System.out.println("------Evaluating Node:"+childNode+"(root:"+rootNode+")");
	  List<String> childNodes = childrenLists.get(childNode); // children of childNode passed - used to be findChildren(childNode); //
	  if (childNodes.size()==0) {    // childNode does not have children
		  // do nothing, return
	  }
	  else {  // childNode has children, call evalChild recursively
		  //each child of childNode
		  for (String childNode2 : childNodes) {
			  evalChild(childNode, childNode2, keysMode);
			  execTransformations(childNode, childNode2);
		  }

		  // combine all edges having as root the childNode in one edge: childNode -> childChildNode
		  // This is done by applying the thetaCombineOp which includes the theta expression of childNode - if exists
		  // this should be the edge that will be joined (rollUp) with rootNode->childNode edge

		  String childChildNode = "childOf"+childNode;

		  String allChildNodes="";
		  boolean isFirst=true;
		  for (String childNode2 : childNodes) {
			  if (!isFirst)
			  	  allChildNodes+=",";
			  isFirst = false;
		  	  allChildNodes+=childNode2;
		  }

		  String outputChildNodes="";
		  isFirst=true;
		  for (String childNode2 : childNodes) {
			  if (outputs.get(childNode2).equals("yes")) {
			  	  if (!isFirst)
			  		  outputChildNodes+=",";
			  	  isFirst = false;
		  	  	  outputChildNodes+=childNode2;
			  }
		  }

		  String theta = thetas.get(childNode);

		  // execute thetaCombine to combine all children of childNode

		  thetaCombine(childNode, childChildNode, allChildNodes, outputChildNodes, theta, keysMode);

		  rollupEdges(rootNode, childNode, childChildNode);

	  } // of else

  } // of evalChild


  //************** This function recursively materializes all the edges of the selected tree. This is done once at the start of the main program
  public static void materializeEdge(String rootNode, List<String> childNodes) throws IOException {
	  for (String childNode: childNodes) {
		  String rootNodeDVM = onNodes.get(rootNode);
		  String childNodeDVM = onNodes.get(childNode);
		  // first materialize rootNode-child edge
		  System.out.println("*** Materializing: "+rootNode+"("+rootNodeDVM+")->"+childNode+"("+childNodeDVM+")");
		  if (jedis.scard(rootNode+"-"+childNode)==0) {
			  System.out.println("    Loading edge from data source");

			  arguments.add(rootNodeDVM);
			  arguments.add(childNodeDVM);
			  arguments.add(rootNode);
			  arguments.add(childNode);

			  /* runtime version
		      try {
				  Process p = Runtime.getRuntime().exec("java loadEdgeNeo4j "+rootNodeDVM+" "+childNodeDVM+" "+rootNode+" "+childNode);
				  p.waitFor();
	      	      int returnValue = p.exitValue();
	      	      if (returnValue!=0) {
					  System.out.println("loadEdgeNeo4j failed for rootNode: "+rootNode+"("+rootNodeDVM+") and childNode: "+childNode+"("+childNodeDVM+") with error code: "+returnValue);
					  System.exit(2);
				  }
		  	  }
		  	  catch (InterruptedException e) {
				  e.printStackTrace();
		  	  }
		  	  */

		  }
		  else {
			  System.out.println("    Edge is already materialized in system");
		  }


		  // then call recursively materializeEdge for each child of childNode
		  List<String> childNodes2 = childrenLists.get(childNode);
		  if (childNodes2.size()!=0)
		  	  materializeEdge(childNode, childNodes2); // recursive call
	  }
  } // of materializeEdge



  //************** MAIN *****************************************************

  public static void main(String[] args) throws ParserConfigurationException,
      XPathExpressionException, org.xml.sax.SAXException, IOException {

	// command arguments should contain:
	// queryFilename - string
	// outputType - string (="excel"/"none") - output always goes to a csv file named $rootNode+$timestamp ("none"). If outputType is set to "excel" it invokes excel to present it
	// keysMode - string : all or intersect

	if (args.length!=3) {
		System.out.println("Wrong number of arguments");
		System.exit(1); // wrong number of args
	}

	String queryFilename = args[0];
	String outputType = args[1];
	String keysMode = args[2];

	if (!outputType.equals("excel") && !outputType.equals("none")) {
		System.out.println("Second argument should be 'excel' or 'none'");
		System.exit(2);
	}

	if (!keysMode.equals("all") && !keysMode.equals("intersect")) {
		System.out.println("Third argument should be 'all' or 'intersect'");
		System.exit(3);
	}

    long startTime = System.currentTimeMillis();

    DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = parser.parse(new File(queryFilename));
    XPath xpath = XPathFactory.newInstance().newXPath();

	int rows=1;

	// populate the childrenLists Map
	rows=1;
	while (xpath.evaluate("/query/node[position()="+rows+"]",doc).trim()!="") {
		String label = xpath.evaluate("/query/node[position()="+rows+"]/label",doc).trim();
		String children = xpath.evaluate("/query/node[position()="+rows+"]/children",doc).trim();
		String[] child = children.split(",",-1);
		List<String> childrenList = new ArrayList<String>();
		if (!children.equals(""))
			for (int i=0; i<child.length; i++) {
				//System.out.println("Child:"+child[i]);
				childrenList.add(child[i]);
			}
		childrenLists.put(label, childrenList);
		rows++;
	}

	// populate the onNodes Map
	rows=1;
	while (xpath.evaluate("/query/node[position()="+rows+"]",doc).trim()!="") {
		String label = xpath.evaluate("/query/node[position()="+rows+"]/label",doc).trim();
		String onNode = xpath.evaluate("/query/node[position()="+rows+"]/onnode",doc).trim();
		onNodes.put(label, onNode);
		rows++;
	}

	// populate the transformations Map
	rows=1;
	while (xpath.evaluate("/query/node[position()="+rows+"]",doc).trim()!="") {
		String label = xpath.evaluate("/query/node[position()="+rows+"]/label",doc).trim();
		String transf = xpath.evaluate("/query/node[position()="+rows+"]/transformations",doc).trim();
		transformations.put(label, transf);
		rows++;
	}

	// populate the thetas Map
	rows=1;
	while (xpath.evaluate("/query/node[position()="+rows+"]",doc).trim()!="") {
		String label = xpath.evaluate("/query/node[position()="+rows+"]/label",doc).trim();
		String theta = xpath.evaluate("/query/node[position()="+rows+"]/theta",doc).trim();
		thetas.put(label, theta);
		rows++;
	}

	// populate the outputs Map
	rows=1;
	while (xpath.evaluate("/query/node[position()="+rows+"]",doc).trim()!="") {
		String label = xpath.evaluate("/query/node[position()="+rows+"]/label",doc).trim();
		String output = xpath.evaluate("/query/node[position()="+rows+"]/output",doc).trim();
		outputs.put(label, output);
		rows++;
	}

	String rootNode = xpath.evaluate("/query/rootnode",doc).trim();

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
    Date date = new Date();
    String dt = formatter.format(date);
    String outFilename = rootNode+"_"+dt+".csv";
    if (queryFilename.equals("tempQuery.qdvm.xml")) { // it is the request from the UI interface - for now
		outFilename = "tempQuery.csv";
	}
	FileWriter outFile = new FileWriter(outFilename);
	PrintWriter out = new PrintWriter(outFile);


	jedis.flushAll(); // no caching, has to be rethought

	List<String> childNodes = childrenLists.get(rootNode);
	materializeEdge(rootNode, childNodes); // it will recursively compute all edges of the tree, actually put the edges that have to be computed in a string

	String[] cmdArgs = new String[arguments.size()];
    arguments.toArray(cmdArgs);
	try {
		loadEdges.main(cmdArgs);
	}
	catch (Exception e) {
		System.out.println("loadEdges failed for query with rootNode: "+rootNode);
		System.exit(4);
	}

	long estimatedTime = System.currentTimeMillis() - startTime;
	System.out.println("Finished loading edges: "+estimatedTime);


	for (String childNode : childNodes) {

		evalChild(rootNode, childNode, keysMode);

		estimatedTime = System.currentTimeMillis() - startTime;
		System.out.println("Finished evaluating child "+childNode+": "+estimatedTime);

		execTransformations(rootNode, childNode);

		estimatedTime = System.currentTimeMillis() - startTime;
		System.out.println("Finished transforming child "+childNode+": "+estimatedTime);

	}

	List<String> outputChildNodes = new ArrayList<String>();;
	for (String childNode : childNodes) {
		if (outputs.get(childNode).equals("yes"))
			outputChildNodes.add(childNode);
	}

	Set<String> keys = combineKeys(rootNode, outputChildNodes, keysMode);

	for (String key : keys) {
		// if (theta(key, thetaChildNodes)==true) then do the following - NOT IMPLEMENTED YET
		out.print("\""+key+"\"");
		for (String childNode : outputChildNodes) {
			out.print(",\"");
			String edge = rootNode+"-"+childNode+":"+key; // the key of the list of rootNode->childNode edge
			List<String> values = jedis.lrange(edge,0,-1);
			boolean started=false;
			for (String value : values) {
				if (started)
					out.print(",");
				out.print(value);
				started=true;
			}
			out.print("\"");
		}
		out.println();
	}

    out.close();
	outFile.close();

	if (outputType.equals("excel")) {
		Process p = Runtime.getRuntime().exec("c:\\Program Files (x86)\\Microsoft Office\\Office12\\excel.exe "+outFilename);
	}

    driver.close();

	estimatedTime = System.currentTimeMillis() - startTime;
	System.out.println("Completed:"+estimatedTime);

  } // of main
} // of program


