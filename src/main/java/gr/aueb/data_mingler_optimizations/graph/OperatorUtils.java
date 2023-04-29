package gr.aueb.data_mingler_optimizations.graph;

import gr.aueb.data_mingler_optimizations.QueryEvaluation;
import gr.aueb.data_mingler_optimizations.enums.Operator;
import gr.aueb.data_mingler_optimizations.exception.OperatorExecutionFailedException;
import gr.aueb.data_mingler_optimizations.exception.TransformationsAreInvalidException;

import java.io.IOException;
import java.util.Arrays;

public class OperatorUtils {

    private static final String SEMI_COLON = ";";
    private static final String COLON = ":";
    private static final String COMMA = ",";
    private static final String NULL = "null";
    private static final String EMPTY = "";

    private static final String AGGREGATE_OPERATOR_COMMAND_TEMPLATE = "java gr.aueb.data_mingler_optimizations.operator.aggregateOp %s %s %s";
    private static final String FILTER_OPERATOR_COMMAND_TEMPLATE = "%s" + "python filterOp.py %s %s \"%s\"";
    private static final String MAP_OPERATOR_COMMAND_TEMPLATE = "%s" + "python gr.aueb.data_mingler_optimizations.operator.mapOp.py %s %s \"%s\" \"%s\"";

    private static void throwExceptionIfTransformationsAreInvalid(String[] transformationsToPerform) {
        if (transformationsToPerform[0].equals(NULL) || transformationsToPerform[0].equals(EMPTY)) {
            throw new TransformationsAreInvalidException();
        }
    }

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
                                        String importPackage, String functionName,
                                        String pythonPath) {
        try {
            String command = String.format(MAP_OPERATOR_COMMAND_TEMPLATE, pythonPath, rootNode, childNode,
                    importPackage, functionName);
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

    public static void executeTransformationOnEdge(String rootNode, String childNode,
                                                   String pythonPath, String importPackage,
                                                   String functionName) {
        String[] transformationsToPerform = QueryEvaluation
                .getTransformations()
                .get(childNode)
                .split(SEMI_COLON, -1);
        throwExceptionIfTransformationsAreInvalid(transformationsToPerform);
        Arrays.stream(transformationsToPerform)
                .forEach(transformation -> {
                    String[] transformationArgs = transformation.split(COLON, -1);
                    String operatorName = transformationArgs[0];
                    String[] operatorParameters = transformationArgs[1].split(COMMA);

                    if (operatorName.equals(Operator.AGGREGATE.name().toLowerCase())) {
                        callAggregateOperator(rootNode, childNode, operatorParameters);
                    } else if (operatorName.equals(Operator.FILTER.name().toLowerCase())){
                        callFilterOperator(rootNode, childNode, operatorParameters, pythonPath);
                    } else if(operatorName.equals(Operator.MAP.name().toLowerCase())) {
                        callMapOperator(rootNode, childNode, importPackage, functionName, pythonPath);
                    }
                });
    }

}