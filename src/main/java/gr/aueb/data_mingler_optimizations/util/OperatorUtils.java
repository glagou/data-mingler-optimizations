package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.QueryEvaluation;
import gr.aueb.data_mingler_optimizations.enumerator.KeyMode;
import gr.aueb.data_mingler_optimizations.enumerator.Operator;
import gr.aueb.data_mingler_optimizations.exception.OperatorExecutionFailedException;
import gr.aueb.data_mingler_optimizations.exception.TransformationsAreInvalidException;
import gr.aueb.data_mingler_optimizations.operator.RollupOperator;

import java.io.IOException;
import java.util.Arrays;

public class OperatorUtils {

    private static final String SEMI_COLON = ";";
    private static final String COLON = ":";
    private static final String COMMA = ",";
    private static final String NULL = "null";
    private static final String EMPTY = "";

    // TODO: Check if package name has to be before operator name
    private static final String AGGREGATE_OPERATOR_COMMAND_TEMPLATE = "java gr.aueb.data_mingler_optimizations.operator.aggregateOp %s %s %s";
    private static final String FILTER_OPERATOR_COMMAND_TEMPLATE = "%s" + "python filterOp.py %s %s \"%s\"";
    private static final String MAP_OPERATOR_COMMAND_TEMPLATE = "%s" + "python gr.aueb.data_mingler_optimizations.operator.mapOp.py %s %s \"%s\" \"%s\"";
    private static final String THETA_COMBINE_OPERATOR_COMMAND_TEMPLATE = "%s" + "python gr.aueb.data_mingler_optimizations.operator.thetaCombineOp.py %s %s \"%s\" \"%s\" \"%s\" \"%s\"";

    private static void validateTransformations(String[] transformationsToPerform) {
        if (transformationsToPerform[0].equals(NULL) || transformationsToPerform[0].trim().equals(EMPTY)) {
            throw new TransformationsAreInvalidException();
        }
    }

    // TODO: We can make this code for calling Processes reusable
    private static void callAggregateOperator(String rootNode, String childNode, String[] operationParameters) {
        try {
            String command = String.format(AGGREGATE_OPERATOR_COMMAND_TEMPLATE, rootNode, childNode,
                    operationParameters[0]);
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
            int returnValue = p.exitValue();
            if (returnValue != 0) {
                throw new OperatorExecutionFailedException(Operator.AGGREGATE);
            }
        } catch (InterruptedException | IOException e) {
            throw new OperatorExecutionFailedException(Operator.AGGREGATE);
        }
    }

    private static void callFilterOperator(String rootNode, String childNode,
                                           String[] operationParameters, String pythonPath) {
        try {
            String command = String.format(FILTER_OPERATOR_COMMAND_TEMPLATE, pythonPath, rootNode, childNode,
                    operationParameters[0]);
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
            int returnValue = p.exitValue();
            if (returnValue != 0) {
                throw new OperatorExecutionFailedException(Operator.FILTER);
            }
        } catch (InterruptedException | IOException e) {
            throw new OperatorExecutionFailedException(Operator.FILTER);
        }
    }

    private static void callMapOperator(String rootNode, String childNode,
                                        String[] operatorParameters, String pythonPath) {
        try {
            String command = String.format(MAP_OPERATOR_COMMAND_TEMPLATE, pythonPath, rootNode, childNode,
                    operatorParameters[1], operatorParameters[2]);
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
            int returnValue = p.exitValue();
            if (returnValue != 0) {
                throw new OperatorExecutionFailedException(Operator.MAP);
            }
        } catch (InterruptedException | IOException e) {
            throw new OperatorExecutionFailedException(Operator.MAP);
        }
    }

    public static void executeThetaCombine(String rootNode, String childNode,
                                           String allChildNodes, String outputChildNodes,
                                           String theta, KeyMode keyMode, String pythonPath) {
        try {
            String command = String.format(THETA_COMBINE_OPERATOR_COMMAND_TEMPLATE, pythonPath, rootNode, childNode,
                    allChildNodes, outputChildNodes, theta, keyMode.name().toLowerCase());
            ProcessBuilder pb = new ProcessBuilder(command);
            Process p = pb.start();
            p.waitFor();
            int returnValue = p.exitValue();
            if (returnValue != 0) {
                throw new OperatorExecutionFailedException(Operator.THETA_COMBINE);
            }

        } catch (InterruptedException | IOException e) {
            throw new OperatorExecutionFailedException(Operator.THETA_COMBINE);
        }

    }

    public static void executeRollupEdges(String rootNode, String childNode, String childChildNode) {
        RollupOperator.main(new String[] { rootNode, childNode, childChildNode });
    }

    // TODO: Path to python can be directly retrieved from main program
    public static void executeTransformationOnEdge(String rootNode, String childNode, String pythonPath) {
        String[] transformationsToPerform = QueryEvaluation
                .getNodeToTransformations()
                .get(childNode)
                .split(SEMI_COLON, -1);
        validateTransformations(transformationsToPerform);
        Arrays.stream(transformationsToPerform)
                .forEach(transformation -> {
                    String[] transformationArgs = transformation.split(COLON, -1);
                    String operatorName = transformationArgs[0];
                    String[] operatorParameters = transformationArgs[1].split(COMMA, -1);
                    if (operatorName.equals(Operator.AGGREGATE.getNameAsCommandLineArgument())) {
                        callAggregateOperator(rootNode, childNode, operatorParameters);
                    } else if (operatorName.equals(Operator.FILTER.getNameAsCommandLineArgument())) {
                        callFilterOperator(rootNode, childNode, operatorParameters, pythonPath);
                    } else if (operatorName.equals(Operator.MAP.getNameAsCommandLineArgument())) {
                        callMapOperator(rootNode, childNode, operatorParameters, pythonPath);
                    }
                });
    }

}
