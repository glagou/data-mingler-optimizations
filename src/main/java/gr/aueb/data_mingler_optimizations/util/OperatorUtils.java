package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.QueryEvaluation;
import gr.aueb.data_mingler_optimizations.enumerator.Operator;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.exception.TransformationsAreInvalidException;
import gr.aueb.data_mingler_optimizations.operator.AggregateOperator;
import gr.aueb.data_mingler_optimizations.operator.RollupOperator;

import java.util.Arrays;

public class OperatorUtils {

    private static void validateTransformations(String[] transformationsToPerform) {
        if (transformationsToPerform[0].equals(StringConstant.NULL.getValue())
                || transformationsToPerform[0].trim().equals(StringConstant.EMPTY.getValue())) {
            throw new TransformationsAreInvalidException();
        }
    }

    private static void callAggregateOperator(String rootNode, String childNode, String[] operationParameters) {
        AggregateOperator.main(new String[]{rootNode, childNode, operationParameters[0]});
    }

    private static void callFilterOperator(String rootNode, String childNode, String[] operationParameters) {
        // TODO: Call Java program with rootNode, childNode, operationParameters[0]
    }

    private static void callMapOperator(String rootNode, String childNode, String[] operatorParameters) {
        // TODO: Call Java program with rootNode, childNode, operatorParameters[1], operatorParameters[2]
    }

    public static void executeThetaCombine(String rootNode, String childNode, String allChildNodes,
                                           String outputChildNodes, String theta) {
        // TODO: Call Java program with rootNode, childNode, allChildNodes, outputChildNodes, theta, keyMode
    }

    public static void executeRollupEdges(String rootNode, String childNode, String childChildNode) {
        RollupOperator.main(new String[]{rootNode, childNode, childChildNode});
    }

    // TODO: Path to python can be directly retrieved from main program
    public static void executeTransformationOnEdge(String rootNode, String childNode) {
        String[] transformationsToPerform = QueryEvaluation
                .getNodeToTransformations()
                .get(childNode)
                .split(StringConstant.SEMI_COLON.getValue(), -1);
        validateTransformations(transformationsToPerform);
        Arrays.stream(transformationsToPerform)
                .forEach(transformation -> {
                    String[] transformationArgs = transformation.split(StringConstant.COLON.getValue(), -1);
                    String operatorName = transformationArgs[0];
                    String[] operatorParameters = transformationArgs[1].split(StringConstant.COMMA.getValue(), -1);
                    if (operatorName.equals(Operator.AGGREGATE.getNameAsCommandLineArgument())) {
                        callAggregateOperator(rootNode, childNode, operatorParameters);
                    } else if (operatorName.equals(Operator.FILTER.getNameAsCommandLineArgument())) {
                        callFilterOperator(rootNode, childNode, operatorParameters);
                    } else if (operatorName.equals(Operator.MAP.getNameAsCommandLineArgument())) {
                        callMapOperator(rootNode, childNode, operatorParameters);
                    }
                });
    }

}
