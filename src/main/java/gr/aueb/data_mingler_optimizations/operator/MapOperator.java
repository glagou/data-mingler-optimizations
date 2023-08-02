package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import org.python.jsr223.PyScriptEngineFactory;

import javax.script.*;
import java.util.Collection;
import java.util.Set;

public class MapOperator {
    private static final ScriptEngineManager manager = new ScriptEngineManager();

    public static void run(String rootNode, String childNode, String functionInvocation) {
        manager.registerEngineExtension("python", new PyScriptEngineFactory());
        ScriptEngine engine = manager.getEngineByName("python");

        String edge = rootNode + "-" + childNode;
        functionInvocation = functionInvocation.replace('$' + childNode + '$', "value");
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            Collection<String> values = GraphUtils.getElements(graphKey);
            GraphUtils.removeElement(graphKey);
            for (String value : values) {
                try {
                    Bindings bindings = new SimpleBindings();
                    bindings.put("value", value);
                    Object result = engine.eval(functionInvocation, bindings);
                    GraphUtils.addValueToCollection(graphKey, String.valueOf(result), GraphAdditionMethod.AS_LIST);
                } catch (ScriptException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
