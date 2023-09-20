package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import gr.aueb.data_mingler_optimizations.util.PythonUtils;
import jep.JepException;
import jep.SharedInterpreter;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class ThetaCombineOperator {
    static SharedInterpreter interpreter = new SharedInterpreter();

    private static boolean evaluateTheta(String theta, String key, String rootNode, String[] allChildNodes) {
        if (theta.equals("True")) return true;
        try {
            for (String childNode : allChildNodes) {
                String element = ((List<String>) GraphUtils.getElements(rootNode + '-' + childNode + ':' + key)).get(0);
                theta = theta.replace('$' + childNode + '$', element);
            }
            interpreter.set("key", key);
            return PythonUtils.evalFromScript(theta, interpreter);
        } catch (JepException e) {
            System.out.println(e.getMessage());
        } finally {
            interpreter.exec("del key");
        }
        return false;
    }

    public static void run(String rootNode, String newChildNode, String allChildNodesCL, String outputChildNodesCL,
                           String thetaCL) {
        Instant start = Instant.now();

        boolean hasOutput = !outputChildNodesCL.isEmpty();
        String[] allChildNodes = allChildNodesCL.split(StringConstant.COMMA.getValue());
        String[] outputChildNodes = outputChildNodesCL.split(StringConstant.COMMA.getValue());

        String theta = "True";
        if (!thetaCL.isEmpty()) {
            theta = thetaCL.replace('$' + rootNode + '$', "key");
        }

        boolean isFirst = true;
        Set<String> keys = new HashSet<>();
        for (String childNode : allChildNodes) {
            if (isFirst) {
                keys = (Set<String>) GraphUtils.getElements(rootNode + "-" + childNode);
                isFirst = false;
                continue;
            }
            keys.retainAll(GraphUtils.getElements(rootNode + "-" + childNode));
        }

        String newEdge = rootNode + '-' + newChildNode;

        for (String key : keys) {
            if (evaluateTheta(theta, key, rootNode, allChildNodes)) {
                GraphUtils.addValueToCollection(newEdge, key, GraphAdditionMethod.AS_SET);
                if (hasOutput) {
                    for (String childNode : outputChildNodes) {
                        String nextEdge = rootNode + '-' + childNode + ':' + key;
                        Collection<String> elements = GraphUtils.getElements(nextEdge);
                        List<String> values = new ArrayList<>(elements);
                        GraphUtils.setCollection(newEdge + ':' + key, values);
                    }
                } else {
                    GraphUtils.addValueToCollection(newEdge + ':' + key, key, GraphAdditionMethod.AS_SET);
                }
            }
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }
}
