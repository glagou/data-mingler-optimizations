package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.QueryEvaluation;
import gr.aueb.data_mingler_optimizations.enumerator.AggregationType;
import gr.aueb.data_mingler_optimizations.enumerator.Operator;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.operator.*;

import java.util.Arrays;

public class OperatorUtils {

    private static void callAggregateOperator(String rootNode, String childNode, String[] operationParameters) {
        AggregationType aggrType = AggregationType.valueOf(operationParameters[0].toUpperCase());
        AggregateOperator.run(rootNode, childNode, aggrType);
    }

    private static void callFilterOperator(String rootNode, String childNode, String[] operationParameters) {
        FilterOperator.run(rootNode, childNode, operationParameters[0]);
    }

    private static void callMapOperator(String rootNode, String childNode, String[] operatorParameters) {
        MapOperator.run(rootNode, childNode, operatorParameters[2]);
    }

    public static void executeThetaCombine(String rootNode, String childNode, String allChildNodes,
                                           String outputChildNodes, String theta) {
        ThetaCombineOperator.run(rootNode, childNode, allChildNodes, outputChildNodes, theta);
    }

    public static void executeRollupEdges(String rootNode, String childNode, String childChildNode) {
        RollupOperator.run(rootNode, childNode, childChildNode);
    }

    public static void executeTransformationOnEdge(String rootNode, String childNode) {
        String[] transformationsToPerform = QueryEvaluation.getNodeToTransformations()
                .get(childNode)
                .split(StringConstant.SEMI_COLON.getValue(), -1);

        if (transformationsToPerform.length > 0 && !transformationsToPerform[0].isEmpty()
                && !transformationsToPerform[0].equalsIgnoreCase(StringConstant.NULL.getValue())) {

            Arrays.stream(transformationsToPerform)
                    .forEach(transformation -> {
                        String[] transformationArgs = transformation.split(StringConstant.COLON.getValue(), -1);
                        String operatorName = transformationArgs[0];
                        String[] operatorParameters = transformationArgs[1].split(StringConstant.COMMA.getValue(), -1);
                        System.out.println("**  Operator: " + operatorName + ", params: " +
                                Arrays.toString(operatorParameters));
                        if (operatorName.equalsIgnoreCase(Operator.AGGREGATE.getNameAsCommandLineArgument())) {
                            callAggregateOperator(rootNode, childNode, operatorParameters);
                        } else if (operatorName.equalsIgnoreCase(Operator.FILTER.getNameAsCommandLineArgument())) {
                            callFilterOperator(rootNode, childNode, operatorParameters);
                        } else if (operatorName.equalsIgnoreCase(Operator.MAP.getNameAsCommandLineArgument())) {
                            callMapOperator(rootNode, childNode, operatorParameters);
                        }
                        System.out.println("Executed transformation with operator: " + operatorName + " and params: "
                                + Arrays.toString(operatorParameters));
                    });

        }

    }

}
