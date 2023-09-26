package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
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
        String pythonCode = script.replace('$' + childNode + '$', "value");

        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            Collection<String> values = GraphUtils.getElements(graphKey);
            GraphUtils.removeElement(graphKey);
            try {
                for (String value : values) {
                    Object result = null;
                    PythonUtils.getInterpreter().set("value", value);
                    result = PythonUtils.getValueFromScript(pythonCode);
                    GraphUtils.addValueToCollection(graphKey, String.valueOf(result), GraphAdditionMethod.AS_LIST);
                }
                PythonUtils.getInterpreter().exec("del value");
            } catch (JepException e) {
                throw new RuntimeException(e);
            }
        }
    }


}
