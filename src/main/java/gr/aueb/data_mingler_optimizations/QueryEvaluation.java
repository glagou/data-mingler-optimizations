package gr.aueb.data_mingler_optimizations;

import gr.aueb.data_mingler_optimizations.exception.PathToPythonNotFoundException;
import gr.aueb.data_mingler_optimizations.load.EdgesLoader;
import gr.aueb.data_mingler_optimizations.operator.rollUpOp;
import org.w3c.dom.Document;

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
// TODO: parallel evals
// TODO: deallocate Redis space (remove KL structures) as soon as possible
// TODO: each edge consists of keys in the form "root-child:key" - could be a tremendous waste of memory space;
//  maybe each edge can get an id (int)
public class QueryEvaluation {

    private static final String PATH_ENV_VAR = "PATH";
    private static final String PYTHON_EXECUTABLE = "python";

    // TODO: Rename these and remove comments after we know what they do
    private static final Map<String, List<String>> childrenLists = new HashMap<>(); // the children's list of each node in the query (global)
    private static final Map<String, String> onNodes = new HashMap<>(); // the actual node in the DVM that a label corresponds to (e.g. X --> custID)
    private static final Map<String, String> transformations = new HashMap<>(); // the transformations' sequence of each node in the query
    private static final Map<String, String> thetas = new HashMap<>(); // the theta expression that has to be expressed on internal nodes in the query
    private static final Map<String, String> outputs = new HashMap<>(); // "yes" or "no" - should the computed edge participate in the output or will it be used just in the theta?
    private static final List<String> arguments = new ArrayList<>(); // list containing the args required for loadEdges command line arguments

    private static String findPathToPython() {
        String pathVariableFromEnv = System.getenv(PATH_ENV_VAR);
        String[] values = pathVariableFromEnv.split(File.pathSeparator);
        return Arrays
                .stream(values)
                .filter(value -> new File(value, PYTHON_EXECUTABLE).exists())
                .findFirst()
                .orElseThrow(PathToPythonNotFoundException::new);
    }

    //************** This function gets a root and its children and combine them into one (new) edge rootNode --> childNode based on a selection expression theta
    public static void thetaCombine(String rootNode, String childNode, String allChildNodes, String outputChildNodes, String theta, String keysMode) throws IOException {

        long starting = System.currentTimeMillis();
        System.out.print("  **  Operator: thetaCombine on:" + rootNode + "(" + allChildNodes + ") - Elapsed time:"); // debug
        try {

            ProcessBuilder pb = new ProcessBuilder(path2Python + "python", "gr.aueb.data_mingler_optimizations.operator.thetaCombineOp.py", "" + rootNode, "" + childNode, "\"" + allChildNodes + "\"", "\"" + outputChildNodes + "\"", "\"" + theta + "\"", "\"" + keysMode + "\"");
            Process p = pb.start();
            // the code bellow is used for debugging python
            // BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            // String inputLine=null;
            // while ((inputLine = reader.readLine()) != null) {
            //     System.out.println("      * Python Output: "+inputLine);
            // }
            int returnValue = p.waitFor();
            if (returnValue != 0) {
                System.out.println("gr.aueb.data_mingler_optimizations.operator.thetaCombineOp failed for rootNode: " + rootNode + ", childNode: " + childNode + " and children: " + allChildNodes + " with error code: " + returnValue);
                System.exit(5);
            }

            //Process p = Runtime.getRuntime().exec("python c:\\datamingler\\implementation\\gr.aueb.data_mingler_optimizations.operator.thetaCombineOp.py "+rootNode+" "+childNode+" \""+allChildNodes+"\" \""+outputChildNodes+"\" \""+theta+"\" \""+keysMode+"\"");

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
            //	System.out.println("gr.aueb.data_mingler_optimizations.operator.thetaCombineOp failed for rootNode: "+rootNode+", childNode: "+childNode+" and children: "+allChildNodes+" with error code: "+returnValue);
            //	System.exit(5);
            //}

        } catch (Exception e) {
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
        System.out.print("  **  Operator: rollUpJoin on:" + rootNode + "->" + childNode + " and " + childNode + "->" + childChildNode + " - Time elapsed: "); // debug

        String[] cmdArgs = {rootNode, childNode, childChildNode};
        try {
            rollUpOp.main(cmdArgs);
        } catch (Exception e) {
            System.out.println("gr.aueb.data_mingler_optimizations.operator.rollUpOp failed for rootNode: " + rootNode + ", childNode: " + childNode + " and childChildNode: " + childChildNode);
            System.exit(5);
        }


	/*
	try {
		Process p = Runtime.getRuntime().exec("java gr.aueb.data_mingler_optimizations.operator.rollUpOp "+rootNode+" "+childNode+" "+childChildNode);
		p.waitFor();
	    int returnValue = p.exitValue();
	    if (returnValue!=0) {
			System.out.println("gr.aueb.data_mingler_optimizations.operator.rollUpOp failed for rootNode: "+rootNode+", childNode: "+childNode+" and childChildNode: "+childChildNode+" with error code: "+returnValue);
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
        System.out.println("------Evaluating Node:" + childNode + "(root:" + rootNode + ")");
        List<String> childNodes = childrenLists.get(childNode); // children of childNode passed - used to be findChildren(childNode); //
        if (childNodes.size() == 0) {    // childNode does not have children
            // do nothing, return
        } else {  // childNode has children, call evalChild recursively
            //each child of childNode
            for (String childNode2 : childNodes) {
                evalChild(childNode, childNode2, keysMode);
                execTransformations(childNode, childNode2);
            }

            // combine all edges having as root the childNode in one edge: childNode -> childChildNode
            // This is done by applying the gr.aueb.data_mingler_optimizations.operator.thetaCombineOp which includes the theta expression of childNode - if exists
            // this should be the edge that will be joined (rollUp) with rootNode->childNode edge

            String childChildNode = "childOf" + childNode;

            String allChildNodes = "";
            boolean isFirst = true;
            for (String childNode2 : childNodes) {
                if (!isFirst)
                    allChildNodes += ",";
                isFirst = false;
                allChildNodes += childNode2;
            }

            String outputChildNodes = "";
            isFirst = true;
            for (String childNode2 : childNodes) {
                if (outputs.get(childNode2).equals("yes")) {
                    if (!isFirst)
                        outputChildNodes += ",";
                    isFirst = false;
                    outputChildNodes += childNode2;
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
        for (String childNode : childNodes) {
            String rootNodeDVM = onNodes.get(rootNode);
            String childNodeDVM = onNodes.get(childNode);
            // first materialize rootNode-child edge
            System.out.println("*** Materializing: " + rootNode + "(" + rootNodeDVM + ")->" + childNode + "(" + childNodeDVM + ")");
            if (jedis.scard(rootNode + "-" + childNode) == 0) {
                System.out.println("    Loading edge from data source");

                arguments.add(rootNodeDVM);
                arguments.add(childNodeDVM);
                arguments.add(rootNode);
                arguments.add(childNode);

            } else {
                System.out.println("    Edge is already materialized in system");
            }


            // then call recursively materializeEdge for each child of childNode
            List<String> childNodes2 = childrenLists.get(childNode);
            if (childNodes2.size() != 0)
                materializeEdge(childNode, childNodes2); // recursive call
        }
    } // of materializeEdge


    //************** MAIN *****************************************************

    public static void main(String[] args) throws ParserConfigurationException,
            XPathExpressionException, org.xml.sax.SAXException, IOException {
        String pathToPython = findPathToPython();
        // command arguments should contain:
        // queryFilename - string
        // outputType - string (="excel"/"none") - output always goes to a csv file named $rootNode+$timestamp ("none"). If outputType is set to "excel" it invokes excel to present it
        // keysMode - string : all or intersect

        if (args.length != 3) {
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

        int rows = 1;

        // populate the childrenLists Map
        rows = 1;
        while (xpath.evaluate("/query/node[position()=" + rows + "]", doc).trim() != "") {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", doc).trim();
            String children = xpath.evaluate("/query/node[position()=" + rows + "]/children", doc).trim();
            String[] child = children.split(",", -1);
            List<String> childrenList = new ArrayList<String>();
            if (!children.equals(""))
                for (int i = 0; i < child.length; i++) {
                    //System.out.println("Child:"+child[i]);
                    childrenList.add(child[i]);
                }
            childrenLists.put(label, childrenList);
            rows++;
        }

        // populate the onNodes Map
        rows = 1;
        while (xpath.evaluate("/query/node[position()=" + rows + "]", doc).trim() != "") {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", doc).trim();
            String onNode = xpath.evaluate("/query/node[position()=" + rows + "]/onnode", doc).trim();
            onNodes.put(label, onNode);
            rows++;
        }

        // populate the transformations Map
        rows = 1;
        while (xpath.evaluate("/query/node[position()=" + rows + "]", doc).trim() != "") {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", doc).trim();
            String transf = xpath.evaluate("/query/node[position()=" + rows + "]/transformations", doc).trim();
            transformations.put(label, transf);
            rows++;
        }

        // populate the thetas Map
        rows = 1;
        while (xpath.evaluate("/query/node[position()=" + rows + "]", doc).trim() != "") {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", doc).trim();
            String theta = xpath.evaluate("/query/node[position()=" + rows + "]/theta", doc).trim();
            thetas.put(label, theta);
            rows++;
        }

        // populate the outputs Map
        rows = 1;
        while (xpath.evaluate("/query/node[position()=" + rows + "]", doc).trim() != "") {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", doc).trim();
            String output = xpath.evaluate("/query/node[position()=" + rows + "]/output", doc).trim();
            outputs.put(label, output);
            rows++;
        }

        String rootNode = xpath.evaluate("/query/rootnode", doc).trim();

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        Date date = new Date();
        String dt = formatter.format(date);
        String outFilename = rootNode + "_" + dt + ".csv";
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
            EdgesLoader.main(cmdArgs);
        } catch (Exception e) {
            System.out.println("loadEdges failed for query with rootNode: " + rootNode);
            System.exit(4);
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Finished loading edges: " + estimatedTime);


        for (String childNode : childNodes) {

            evalChild(rootNode, childNode, keysMode);

            estimatedTime = System.currentTimeMillis() - startTime;
            System.out.println("Finished evaluating child " + childNode + ": " + estimatedTime);

            execTransformations(rootNode, childNode);

            estimatedTime = System.currentTimeMillis() - startTime;
            System.out.println("Finished transforming child " + childNode + ": " + estimatedTime);

        }

        List<String> outputChildNodes = new ArrayList<String>();
        ;
        for (String childNode : childNodes) {
            if (outputs.get(childNode).equals("yes"))
                outputChildNodes.add(childNode);
        }

        Set<String> keys = combineKeys(rootNode, outputChildNodes, keysMode);

        for (String key : keys) {
            // if (theta(key, thetaChildNodes)==true) then do the following - NOT IMPLEMENTED YET
            out.print("\"" + key + "\"");
            for (String childNode : outputChildNodes) {
                out.print(",\"");
                String edge = rootNode + "-" + childNode + ":" + key; // the key of the list of rootNode->childNode edge
                List<String> values = jedis.lrange(edge, 0, -1);
                boolean started = false;
                for (String value : values) {
                    if (started)
                        out.print(",");
                    out.print(value);
                    started = true;
                }
                out.print("\"");
            }
            out.println();
        }

        out.close();
        outFile.close();

        if (outputType.equals("excel")) {
            Process p = Runtime.getRuntime().exec("c:\\Program Files (x86)\\Microsoft Office\\Office12\\excel.exe " + outFilename);
        }

        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Completed:" + estimatedTime);

    } // of main

    public static Map<String, String> getTransformations() {
        return transformations;
    }
} // of program


