package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.python.Script;
import gr.aueb.data_mingler_optimizations.singleton.PythonInterpreterSingleton;
import gr.aueb.data_mingler_optimizations.util.PythonUtils;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import jep.JepException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapOperator {

    public static void run(String rootNode, String childNode, String script) {
        Script pythonCode = new Script(script);
        pythonCode.renameScriptVariable('$' + childNode + '$', "value");

        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            Collection<String> values = GraphUtils.getElements(graphKey);
            GraphUtils.removeElement(graphKey);
            try {
                for (String value : values) {
                    Object result;
                    PythonInterpreterSingleton.getInterpreter().set("value", value);
                    result = PythonUtils.getValueFromScript(pythonCode.getScript());
                    if (result != null && result instanceof Number) {
                        String resultString = String.valueOf(result);

                        // Check if the result is in scientific notation
                        if (resultString.toLowerCase().contains("e")) {
                            continue; // Skip adding this value
                        }

                        GraphUtils.addValueToCollection(graphKey, resultString, GraphAdditionMethod.AS_LIST);
                    }
                }
                PythonInterpreterSingleton.getInterpreter().exec("del value");
            } catch (JepException e) {
                throw new RuntimeException(e);
            }
        }
    }


}
