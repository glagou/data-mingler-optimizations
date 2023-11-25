package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.python.Script;
import gr.aueb.data_mingler_optimizations.util.PythonUtils;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import jep.Interpreter;
import jep.JepException;
import jep.SharedInterpreter;

import java.util.Collection;
import java.util.Set;

public class MapOperator {

    public static void run(String rootNode, String childNode, String script) {

        Script pythonCode = new Script(script);
        pythonCode.renameScriptVariable('$' + childNode + '$', "value");

        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);

            keys.parallelStream().forEach(key -> {
                String graphKey = edge + ":" + key;
                Collection<String> values = GraphUtils.getElements(graphKey);
                GraphUtils.removeElement(graphKey);
                if (values == null) return;
                try (Interpreter interpreter = new SharedInterpreter()) {
                    for (String value : values) {
                        Object result;
                        interpreter.set("value", value);
                        result = PythonUtils.getValueFromScript(interpreter, pythonCode);
                        if (result instanceof Number) {
                            String resultString = String.valueOf(result);
                            // Check if the result is in scientific notation
                            if (resultString.toLowerCase().contains("e")) {
                                continue; // Skip adding this value
                            }

                            GraphUtils.addValueToCollection(graphKey, resultString, GraphAdditionMethod.AS_LIST);
                        }
                    }
                } catch (JepException e) {
                    throw new RuntimeException(e);
                }
            });

    }
}
