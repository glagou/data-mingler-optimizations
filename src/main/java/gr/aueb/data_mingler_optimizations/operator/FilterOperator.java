package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import gr.aueb.data_mingler_optimizations.util.PythonUtils;
import jep.JepException;
import jep.SharedInterpreter;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

public class FilterOperator {
    static SharedInterpreter interpreter = new SharedInterpreter();

    public static void run(String rootNode, String childNode, String expressionCL) {
        String expression = expressionCL.replace('$' + childNode + '$', "Lvalue");
        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            Collection<String> values = GraphUtils.getElements(graphKey);
            GraphUtils.removeElement(graphKey);
            for (String value : values) {
                try {
                    interpreter.set("Lvalue", value);
                    boolean result = PythonUtils.evalFromScript(expression, interpreter);
                    if (result) {
                        GraphUtils.addValueToCollection(graphKey, value, GraphAdditionMethod.AS_LIST);
                    }
                } catch (JepException e) {
                    throw new RuntimeException(e);
                }
            }
            interpreter.exec("del Lvalue");
        }
    }
}
