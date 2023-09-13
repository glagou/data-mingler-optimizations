package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import jep.JepException;
import jep.SharedInterpreter;

import java.util.Collection;
import java.util.Set;

public class MapOperator {
    static SharedInterpreter interpreter = new SharedInterpreter();

    public static void run(String rootNode, String childNode, String functionInvocation) {
        String edge = rootNode + "-" + childNode;
        functionInvocation = functionInvocation.replace('$' + childNode + '$', "value");
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            Collection<String> values = GraphUtils.getElements(graphKey);
            GraphUtils.removeElement(graphKey);

            for (String value : values) {
                try {
                    interpreter.set("value", value);
                    Object result = interpreter.getValue(functionInvocation);
                    GraphUtils.addValueToCollection(graphKey, String.valueOf(result), GraphAdditionMethod.AS_LIST);
                } catch (JepException e) {
                    throw new RuntimeException(e);
                }
            }
            interpreter.exec("del value");
        }
    }
}
