package gr.aueb.data_mingler_optimizations;

import gr.aueb.data_mingler_optimizations.enums.KeyMode;
import gr.aueb.data_mingler_optimizations.enums.Output;
import gr.aueb.data_mingler_optimizations.enums.OutputType;
import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.exception.LoadEdgesExecutionFailedException;
import gr.aueb.data_mingler_optimizations.exception.PathToPythonNotFoundException;
import gr.aueb.data_mingler_optimizations.exception.UnableToInitializeDocumentAndXpathException;
import gr.aueb.data_mingler_optimizations.load.EdgesLoader;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import gr.aueb.data_mingler_optimizations.util.OperatorUtils;
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
 *     <li>keysMode - {@link KeyMode}</li>
 * </ul>
 * <br>
 */
// TODO: parallel evals
// TODO: each edge consists of keys in the form "root-child:key" - could be a tremendous waste of memory space;
//  maybe each edge can get an id (int)
public class QueryEvaluation {

    private static final String PATH_ENV_VAR = "PATH";
    private static final String PYTHON_EXECUTABLE = "python";
    private static final String CHILD_OF_PREFIX = "childOf";
    private static final String PATH_TO_EXCEL = "c:\\Program Files (x86)\\Microsoft Office\\Office12\\excel.exe";

    private static final Map<String, List<String>> nodeToChildrenNodes = new HashMap<>();
    private static final Map<String, String> dvmNodesToActualLabels = new HashMap<>();
    private static final Map<String, String> nodeToTransformations = new HashMap<>();
    private static final Map<String, String> thetasOnInternalNodes = new HashMap<>();
    private static final Map<String, String> outputs = new HashMap<>();
    private static final List<String> loadEdgesCmdArgs = new ArrayList<>();

    private static String pathToPython;
    private static String queryFilename;
    private static OutputType outputType;
    private static KeyMode keysMode;
    private static Document document;
    private static XPath xpath;
    private static String rootNode;
    private static List<String> childNodes;

    private static void initializePathToPython() {
        String pathVariableFromEnv = System.getenv(PATH_ENV_VAR);
        String[] values = pathVariableFromEnv.split(File.pathSeparator);
        pathToPython = Arrays
                .stream(values)
                .filter(value -> new File(value, PYTHON_EXECUTABLE).exists())
                .findFirst()
                .orElseThrow(PathToPythonNotFoundException::new);
    }

    private static void validateCmdArguments(String[] args) {
        if (args.length != 3) {
            throw new InvalidNumberOfCmdArgumentsException();
        }
    }

    private static void initializeValuesFromCmdArguments(String[] args) {
        queryFilename = args[0];
        outputType = OutputType.valueOf(args[1]);
        keysMode = KeyMode.valueOf(args[2]);
    }

    private static void initializeDocumentAndXpath() {
        try {
            DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            document = parser.parse(new File(queryFilename));
        } catch (SAXException | IOException | ParserConfigurationException e) {
            throw new UnableToInitializeDocumentAndXpathException(queryFilename);
        }
        xpath = XPathFactory.newInstance().newXPath();
    }

    private static void populateNodeToChildrenNodes() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", document).trim().equals("")) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", document).trim();
            String children = xpath.evaluate("/query/node[position()=" + rows + "]/children", document).trim();
            String[] child = children.split(",", -1);
            List<String> childrenList = new ArrayList<String>();
            if (!children.equals("")) {
                childrenList.addAll(Arrays.asList(child));
            }
            nodeToChildrenNodes.put(label, childrenList);
            rows++;
        }
    }

    private static void populateDvmNodesToActualLabels() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", document).trim().equals("")) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", document).trim();
            String onNode = xpath.evaluate("/query/node[position()=" + rows + "]/onnode", document).trim();
            dvmNodesToActualLabels.put(label, onNode);
            rows++;
        }
    }

    private static void populateNodeToTransformations() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", document).trim().equals("")) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", document).trim();
            String transf = xpath.evaluate("/query/node[position()=" + rows + "]/transformations", document).trim();
            nodeToTransformations.put(label, transf);
            rows++;
        }
    }

    private static void populateThetasOnInternalNodes() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", document).trim().equals("")) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", document).trim();
            String theta = xpath.evaluate("/query/node[position()=" + rows + "]/theta", document).trim();
            thetasOnInternalNodes.put(label, theta);
            rows++;
        }
    }

    private static void populateOutputs() throws XPathExpressionException {
        int rows = 1;
        while (!xpath.evaluate("/query/node[position()=" + rows + "]", document).trim().equals("")) {
            String label = xpath.evaluate("/query/node[position()=" + rows + "]/label", document).trim();
            String output = xpath.evaluate("/query/node[position()=" + rows + "]/output", document).trim();
            outputs.put(label, output);
            rows++;
        }
    }

    private static void initializeRootNodeAndChildNodes() throws XPathExpressionException {
        rootNode = xpath.evaluate("/query/rootnode", document).trim();
        childNodes = nodeToChildrenNodes.get(rootNode);
    }

    private static void materializeEdge(String rootNode, List<String> childNodes) {
        childNodes.forEach(childNode -> {

            String rootNodeDVM = dvmNodesToActualLabels.get(rootNode);
            String childNodeDVM = dvmNodesToActualLabels.get(childNode);

            if (GraphUtils.getNumberOfElements(rootNode, childNode) == 0) {
                loadEdgesCmdArgs.add(rootNodeDVM);
                loadEdgesCmdArgs.add(childNodeDVM);
                loadEdgesCmdArgs.add(rootNode);
                loadEdgesCmdArgs.add(childNode);
            }

            List<String> childrenOfChildNode = nodeToChildrenNodes.get(childNode);
            if (childrenOfChildNode.size() != 0) {
                materializeEdge(childNode, childrenOfChildNode);
            }
        });
    }

    private static void loadEdges() {
        String[] cmdArgs = new String[loadEdgesCmdArgs.size()];
        loadEdgesCmdArgs.toArray(cmdArgs);
        try {
            EdgesLoader.main(cmdArgs);
        } catch (Exception e) {
            throw new LoadEdgesExecutionFailedException(rootNode);
        }

    }

    private static void evaluateChild(String rootNode, String childNode) {
        List<String> childrenOfChildNode = nodeToChildrenNodes.get(childNode);
        if (childrenOfChildNode.size() > 0) {
            childrenOfChildNode.forEach(childOfChildNode -> {
                evaluateChild(childNode, childOfChildNode);
                OperatorUtils.executeTransformationOnEdge(childNode, childOfChildNode, pathToPython);
            });

            String childOfChildNode = CHILD_OF_PREFIX.concat(childNode);

            StringBuilder allChildNodes = new StringBuilder();
            boolean isFirst = true;
            for (String childNode2 : childrenOfChildNode) {
                if (!isFirst)
                    allChildNodes.append(",");
                isFirst = false;
                allChildNodes.append(childNode2);
            }

            StringBuilder outputChildNodes = new StringBuilder();
            isFirst = true;
            for (String childNode2 : childrenOfChildNode) {
                if (outputs.get(childNode2).equals("yes")) {
                    if (!isFirst)
                        outputChildNodes.append(",");
                    isFirst = false;
                    outputChildNodes.append(childNode2);
                }
            }

            String theta = thetasOnInternalNodes.get(childNode);

            OperatorUtils.executeThetaCombine(childNode, childOfChildNode, allChildNodes.toString(),
                    outputChildNodes.toString(), theta, keysMode, pathToPython);
            OperatorUtils.executeRollupEdges(rootNode, childNode, childOfChildNode);
        }
    }

    private static void evaluateChildAndExecuteTransformations() {
        childNodes.forEach(childNode -> {
            evaluateChild(rootNode, childNode);
            OperatorUtils.executeTransformationOnEdge(rootNode, childNode, pathToPython);
        });
    }

    private static List<String> initializeOutputChildNodes() {
        List<String> outputChildNodes = new ArrayList<>();
        childNodes.forEach(childNode -> {
            if (outputs.get(childNode).equals(Output.YES.name().toLowerCase())) {
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
        if (queryFilename.equals("tempQuery.qdvm.xml")) {
            outFilename = "tempQuery.csv";
        }
        FileWriter outFile = new FileWriter(outFilename);
        PrintWriter out = new PrintWriter(outFile);
        for (String key : keys) {
            out.print("\"" + key + "\"");
            for (String childNode : outputChildNodes) {
                out.print(",\"");
                String edge = rootNode + "-" + childNode + ":" + key;
                Set<String> values = GraphUtils.getElements(edge);
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
        return outFilename;
    }

    private static void openWithExcelIfNeeded(String outFilename) throws IOException {
        if (outputType.equals(OutputType.EXCEL)) {
            Runtime.getRuntime().exec(PATH_TO_EXCEL + " " + outFilename);
        }
    }

    public static void main(String[] args) throws IOException, XPathExpressionException {
        initializePathToPython();
        validateCmdArguments(args);
        initializeValuesFromCmdArguments(args);
        initializeDocumentAndXpath();
        populateNodeToChildrenNodes();
        populateDvmNodesToActualLabels();
        populateNodeToTransformations();
        populateThetasOnInternalNodes();
        populateOutputs();
        initializeRootNodeAndChildNodes();
        materializeEdge(rootNode, childNodes);
        loadEdges();
        evaluateChildAndExecuteTransformations();
        List<String> outputChildNodes = initializeOutputChildNodes();
        Set<String> keys = GraphUtils.combineKeys(rootNode, outputChildNodes, keysMode);
        String outputFileName = createOutputFile(keys, outputChildNodes);
        openWithExcelIfNeeded(outputFileName);
    }

    public static Map<String, String> getNodeToTransformations() {
        return nodeToTransformations;
    }

}


