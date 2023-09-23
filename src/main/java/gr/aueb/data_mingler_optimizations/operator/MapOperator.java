package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.util.PythonUtils;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import jep.Interpreter;
import jep.JepException;
import jep.SharedInterpreter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapOperator {
    static final Pattern importPattern = Pattern.compile("^import .*|^from .* import .*");


    public static void run(String rootNode, String childNode, String script) {
        String pythonCode = getPythonCode(script).replace('$' + childNode + '$', "value");

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

    static String getPythonCode(String script) {
        if (!script.endsWith(".py")) return script;

        StringBuilder importsBuilder = new StringBuilder();
        StringBuilder codeBuilder = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(script));
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = importPattern.matcher(line);
                if (matcher.matches()) {
                    importsBuilder.append(line).append("\n");
                } else {
                if (!line.trim().isEmpty()) {
                    codeBuilder.append(line).append("\n");
                }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        PythonUtils.importOnceToInterpreter(importsBuilder.toString());
        return codeBuilder.toString();
    }
}
