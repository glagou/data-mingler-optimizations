package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import javax.script.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class FilterOperator {
    private static final ScriptEngineManager manager = new ScriptEngineManager();
    private static final ScriptEngine engine = manager.getEngineByName("python");

    private static void validateCmdArguments(String[] args) {
        if (args.length != 3) {
            throw new InvalidNumberOfCmdArgumentsException();
        }
    }

    public static void main(String[] args) throws InvalidNumberOfCmdArgumentsException {

        validateCmdArguments(args);

        String rootNode = args[0];
        String childNode = args[1];
        String expressionCL = args[2];
        String expression = expressionCL.replace('$'+childNode+'$',"value");

        Instant start = Instant.now();

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
            GraphUtils.putValue(edge+":"+key,newValues);
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }
}
