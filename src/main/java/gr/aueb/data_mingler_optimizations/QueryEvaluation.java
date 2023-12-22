package gr.aueb.data_mingler_optimizations;

import gr.aueb.data_mingler_optimizations.enumerator.KeysMode;
import gr.aueb.data_mingler_optimizations.enumerator.Output;
import gr.aueb.data_mingler_optimizations.enumerator.OutputType;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.exception.LoadEdgesExecutionFailedException;
import gr.aueb.data_mingler_optimizations.exception.UnableToInitializeDocumentAndXpathException;
import gr.aueb.data_mingler_optimizations.load.EdgesLoader;
import gr.aueb.data_mingler_optimizations.load.EnvLoader;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import gr.aueb.data_mingler_optimizations.util.OperatorUtils;
import jep.MainInterpreter;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;


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
 * For successful execution of this class, the following command line arguments should be provided:
 * <ul>
 *     <li>queryFilename - {@link String}</li>
 *     <li>outputType - {@link OutputType} (output always goes to a csv file named $rootNode+$timestamp ("none").
 *     If outputType is set to "excel" it invokes excel to present it)</li>
 *     <li>keysMode - {@link KeysMode}</li>
 * </ul>
 * <br>
 */
public class QueryEvaluation {

    private static final String PATH_TO_EXCEL = "\"C:\\Program Files (x86)\\Microsoft Office\\root\\Office16\\EXCEL.EXE\"";

    private static final Map<String, List<String>> NODE_TO_CHILDREN_NODES = new HashMap<>();
    private static final Map<String, String> DVM_NODES_TO_LABELS = new HashMap<>();
    private static final ConcurrentMap<String, String> NODE_TO_TRANSFORMATIONS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, String> THETAS_ON_INTERNAL_NODES = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> OUTPUTS = new HashMap<>();
    private static final List<String> LOAD_EDGES_CMD_ARGS = new ArrayList<>();

    private static String queryFilePath;
    private static OutputType outputType;
    private static KeysMode keysMode;
    private static Document queryDocument;
    private static XPath xpath;
    private static String rootNode;
    private static List<String> branchRootNodes;

    private static void validateCmdArguments(String[] args) {
        if (args.length != 3) {
            throw new InvalidNumberOfCmdArgumentsException();
        }
    }

    private static void initializeValuesFromCmdArguments(String[] args) {
        queryFilePath = args[0];
        outputType = OutputType.valueOf(args[1].toUpperCase());
        keysMode = KeysMode.valueOf(args[2].toUpperCase());
    }

    private static void initializeDocumentAndXpath() {
        try {
            DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            queryDocument = parser.parse(new File(queryFilePath));
        } catch (SAXException | IOException | ParserConfigurationException e) {
            throw new UnableToInitializeDocumentAndXpathException(queryFilePath);
        }
        xpath = XPathFactory.newInstance().newXPath();
    }

    private static void populateNodeToChildrenNodes() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", queryDocument).trim().isEmpty()) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", queryDocument).trim();
            String children = xpath.evaluate("/query/node[position()=" + rows + "]/children", queryDocument).trim();
            String[] child = children.split(",", -1);
            List<String> childrenList = new ArrayList<>();
            if (!children.isEmpty()) {
                childrenList.addAll(Arrays.asList(child));
            }
            NODE_TO_CHILDREN_NODES.put(label, childrenList);
            rows++;
        }
    }

    private static void populateDvmNodesToLabels() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", queryDocument).trim().isEmpty()) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", queryDocument).trim();
            String onNode = xpath.evaluate("/query/node[position()=" + rows + "]/onnode", queryDocument).trim();
            DVM_NODES_TO_LABELS.put(label, onNode);
            rows++;
        }
    }

    private static void populateNodeToTransformations() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", queryDocument).trim().isEmpty()) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", queryDocument).trim();
            String transf = xpath.evaluate("/query/node[position()=" + rows + "]/transformations", queryDocument).trim();
            NODE_TO_TRANSFORMATIONS.put(label, transf);
            rows++;
        }
    }

    private static void populateThetasOnInternalNodes() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", queryDocument).trim().isEmpty()) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", queryDocument).trim();
            String theta = xpath.evaluate("/query/node[position()=" + rows + "]/theta", queryDocument).trim();
            THETAS_ON_INTERNAL_NODES.put(label, theta);
            rows++;
        }
    }

    private static void populateOutputs() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", queryDocument).trim().isEmpty()) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", queryDocument).trim();
            String output = xpath.evaluate("/query/node[position()=" + rows + "]/output", queryDocument).trim();
            OUTPUTS.put(label, output.equalsIgnoreCase(Output.YES.getValue()));
            rows++;
        }
    }

    private static void initializeRootNodeAndChildNodes() throws XPathExpressionException {
        rootNode = xpath.evaluate("/query/rootnode", queryDocument).trim();
        branchRootNodes = NODE_TO_CHILDREN_NODES.get(rootNode);
    }

    private static void materializeEdge(String rootNode, List<String> childNodes) {
        childNodes.forEach(childNode -> {
            String rootNodeDVM = DVM_NODES_TO_LABELS.get(rootNode);
            String childNodeDVM = DVM_NODES_TO_LABELS.get(childNode);
            System.out.println("*** Materializing: " + rootNode + "(" + rootNodeDVM + ")->" + childNode +
                    "(" + childNodeDVM + ")");

            String key = rootNode + "-" + childNode;
            if (GraphUtils.getElements(key) == null || GraphUtils.getElements(key).isEmpty()) {
                System.out.println("Loading edge from data source: " + key);
                LOAD_EDGES_CMD_ARGS.add(rootNodeDVM);
                LOAD_EDGES_CMD_ARGS.add(childNodeDVM);
                LOAD_EDGES_CMD_ARGS.add(rootNode);
                LOAD_EDGES_CMD_ARGS.add(childNode);
            } else {
                System.out.println("Edge: " + key + " is already materialized in system");
            }

            List<String> childrenOfChildNode = NODE_TO_CHILDREN_NODES.get(childNode);
            if (!childrenOfChildNode.isEmpty()) {
                materializeEdge(childNode, childrenOfChildNode);
            }
        });
    }

    private static void loadEdges() {
        try {
            String[] cmdArgs = LOAD_EDGES_CMD_ARGS.toArray(new String[0]);
            EdgesLoader.main(cmdArgs);
        } catch (Exception e) {
            throw new LoadEdgesExecutionFailedException(rootNode, e);
        }
    }


    private static void evaluateChild(String rootNode, String childNode) {
        System.out.println("------ Evaluating Node: " + childNode + "(root:" + rootNode + ")");
        List<String> childrenOfChildNode = NODE_TO_CHILDREN_NODES.get(childNode);
        if (childrenOfChildNode.isEmpty()) return;

        childrenOfChildNode.forEach(childOfChildNode -> {
            evaluateChild(childNode, childOfChildNode);
            OperatorUtils.executeTransformationOnEdge(childNode, childOfChildNode);
        });

        StringBuilder allChildNodes = new StringBuilder();
        childrenOfChildNode.forEach(childOfChildNodeIter -> {
            allChildNodes.append(childOfChildNodeIter);
            allChildNodes.append(StringConstant.COMMA.getValue());
        });

        StringBuilder outputChildNodes = new StringBuilder();
        childrenOfChildNode.forEach(childOfChildNodeIter -> {
            if (OUTPUTS.get(childOfChildNodeIter)) {
                outputChildNodes.append(childOfChildNodeIter);
                outputChildNodes.append(StringConstant.COMMA.getValue());
            }

        });

        String theta = THETAS_ON_INTERNAL_NODES.get(childNode);
        String childOfChildNode = StringConstant.CHILD_OF_PREFIX.getValue().concat(childNode);
        OperatorUtils.executeThetaCombine(childNode, childOfChildNode, allChildNodes.toString(),
                outputChildNodes.toString(), theta);
        OperatorUtils.executeRollupEdges(rootNode, childNode, childOfChildNode);
    }

    private static void evaluateChildAndExecuteTransformations() {
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        try {

            // Create a parallel stream to evaluate and transform child nodes concurrently
            forkJoinPool.submit(() -> {
                branchRootNodes.forEach(branchRootNode -> {
                    System.out.println("Thread " + Thread.currentThread().getId() + " working on node: " + branchRootNode);
                    evaluateChild(rootNode, branchRootNode);
                    System.out.println(" - Finished evaluating child: " + branchRootNode);
                    OperatorUtils.executeTransformationOnEdge(rootNode, branchRootNode);
                    System.out.println(" - Finished transforming child: " + branchRootNode);
                    System.out.println("Thread " + Thread.currentThread().getId() + " finished working on node: " + branchRootNode);
                });
            }).join();
        } catch (Exception e) {
            throw new RuntimeException("Operations on branches encountered error: " + e);
        } finally {
            forkJoinPool.shutdown();
        }
    }


    private static List<String> initializeOutputChildNodes() {
        List<String> outputChildNodes = new ArrayList<>();
        branchRootNodes.forEach(childNode -> {
            if (OUTPUTS.get(childNode)) {
                outputChildNodes.add(childNode);
            }
        });
        return outputChildNodes;
    }

    private static String createOutputFile(Set<String> keys, List<String> outputChildNodes) throws IOException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        Date date = new Date();
        String dt = formatter.format(date);
        String outFilename = rootNode + "_" + dt + ".csv";
        if (queryFilePath.equals("tempQuery.qdvm.xml")) {
            outFilename = "tempQuery.csv";
        }
        FileWriter outFile = new FileWriter(outFilename);
        PrintWriter out = new PrintWriter(outFile);
        for (String key : keys) {
            out.print("\"" + key + "\"");
            for (String childNode : outputChildNodes) {
                out.print(",\"");
                String edge = rootNode + "-" + childNode + ":" + key;
                Collection<String> values = GraphUtils.getElements(edge);
                if (values != null) {
                    boolean started = false;
                    for (String value : values) {
                        if (started)
                            out.print(",");
                        out.print(value);
                        started = true;
                    }
                }
                out.print("\"");
            }
            out.println();
        }

        out.close();
        outFile.close();
        return outFilename;
    }

    private static void openWithExcelIfNeeded(String outFilename) throws IOException {
        if (outputType == OutputType.EXCEL) {
            String absoluteFilePath = new File(outFilename).getAbsolutePath();
            ProcessBuilder processBuilder = new ProcessBuilder(PATH_TO_EXCEL + " " + "\""
                    + absoluteFilePath + "\"");
            processBuilder.start();
        }
    }

    public static void main(String[] args) throws IOException, XPathExpressionException {
        long startTime = System.currentTimeMillis();
        EnvLoader.load();
        MainInterpreter.setJepLibraryPath(System.getProperty("JEP_PATH"));
        validateCmdArguments(args);
        initializeValuesFromCmdArguments(args);
        initializeDocumentAndXpath();
        populateNodeToChildrenNodes();
        populateDvmNodesToLabels();
        populateNodeToTransformations();
        populateThetasOnInternalNodes();
        populateOutputs();
        initializeRootNodeAndChildNodes();
        materializeEdge(rootNode, branchRootNodes);
        loadEdges();
        System.out.println("Finished loading edges");
        evaluateChildAndExecuteTransformations();
        List<String> outputChildNodes = initializeOutputChildNodes();
        Set<String> keys = (Set<String>) GraphUtils.combineKeys(rootNode, outputChildNodes);
        String outputFileName = createOutputFile(keys, outputChildNodes);
        openWithExcelIfNeeded(outputFileName);
        long endTime = System.currentTimeMillis();
        long executionTimeInMillis = endTime - startTime;
        long executionTimeInSecs = executionTimeInMillis / 1000;
        long mins = executionTimeInSecs / 60;
        long secs = executionTimeInSecs % 60;
        // Print the execution time in minutes and seconds
        System.out.println("Execution time: " + mins + " minutes " + secs + " seconds");
    }

    public static Map<String, String> getNodeToTransformations() {
        return NODE_TO_TRANSFORMATIONS;
    }

    public static KeysMode getKeysMode() {
        return keysMode;
    }

}


