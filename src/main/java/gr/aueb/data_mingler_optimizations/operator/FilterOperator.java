package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import org.python.jsr223.PyScriptEngineFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Collection;
import java.util.Set;

public class FilterOperator {

    private static final ScriptEngineManager manager = new ScriptEngineManager();

    public static void run(String rootNode, String childNode, String expressionCL) {
        manager.registerEngineExtension("python", new PyScriptEngineFactory());
        ScriptEngine engine = manager.getEngineByName("python");

        String expression = expressionCL.replace('$' + childNode + '$', "Lvalue");
        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            Collection<String> values = GraphUtils.getElements(edge);
            GraphUtils.removeElement(graphKey);
            for (String value : values) {
                try {
                    Object result = engine.eval(expression);
                    if (result instanceof Boolean && (Boolean) result) {
                        GraphUtils.addValueToCollection(graphKey, value, GraphAdditionMethod.AS_LIST);
                    }
                } catch (ScriptException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
