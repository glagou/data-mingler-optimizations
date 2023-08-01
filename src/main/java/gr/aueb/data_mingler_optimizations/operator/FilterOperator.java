package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import org.python.jsr223.PyScriptEngineFactory;

import javax.script.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class FilterOperator {
    private static final ScriptEngineManager manager = new ScriptEngineManager();

    public static void run(String rootNode, String childNode, String expressionCL) {
        manager.registerEngineExtension("python", new PyScriptEngineFactory());
        ScriptEngine engine = manager.getEngineByName("python");
        Instant start = Instant.now();
        String expression = expressionCL.replace('$'+childNode+'$',"value");

        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        for (String key : keys) {
            Collection<String> values = GraphUtils.getElements(edge);
            GraphUtils.removeElement(edge.concat(":").concat(key));
            List<String> newValues = new ArrayList<>();
            for (String value : values) {
                try {
                    Bindings bindings = new SimpleBindings();
                    bindings.put("value", value);
                    Object result = engine.eval(expression, bindings);
                    if (result instanceof Boolean && (Boolean) result) {
                        newValues.add(result.toString());
                    }

                } catch (ScriptException e) {
                    System.out.println(e.getMessage());
                }
            }
            GraphUtils.setCollection(edge+":"+key,newValues);
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }
}
