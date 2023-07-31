package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import javax.script.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class ThetaCombineOperator {
    private static final ScriptEngineManager manager = new ScriptEngineManager();
    private static final ScriptEngine engine = manager.getEngineByName("python");

    public static void run(String rootNode, String newChildNode, String allChildNodesCL, String outputChildNodesCL,
                            String thetaCL) {

        Instant start = Instant.now();

        boolean hasOutput = !outputChildNodesCL.isEmpty();
        String[] allChildNodes = allChildNodesCL.split(StringConstant.COMMA.getValue());
        String[] outputChildNodes = outputChildNodesCL.split(StringConstant.COMMA.getValue());

        String theta = "True";
        if (!thetaCL.isEmpty()) {
            theta = thetaCL.replace('$'+rootNode+'$', "key");
            for (String childNode : allChildNodes) {
                // TODO: Fix by adding "key" binding somehow. Originally was: "r.lrange(\""+rootNode+'-'+childNode+":\"+key,0,1)[0]"
                theta = theta.replace(
                        '$'+childNode+'$', ((List<String>) GraphUtils.getElements(rootNode+'-'+childNode)).get(0)
                );
            }
        }

        boolean isFirst = true;
        Set<String> keys = new HashSet<>();
        for (String childNode : allChildNodes) {
            String edge = rootNode + "-" + childNode;
            if  (isFirst) {
                keys = (Set<String>) GraphUtils.getElements(edge);
                isFirst = false;
            }
            keys.retainAll(GraphUtils.getElements(edge));
        }

        String newEdge = rootNode + '-' + newChildNode;

        for (String key : keys) {
            try {
                Bindings bindings = new SimpleBindings();
                bindings.put("key", key);
                Object result = engine.eval(theta, bindings);
                if (result instanceof Boolean && (Boolean) result) {
                    GraphUtils.addValueToCollection(newEdge, key);
                    if (hasOutput) {
                        for (String childNode : outputChildNodes) {
                            String nextEdge = rootNode + '-' + childNode + ':' + key;
                            List<String> values = new ArrayList<>(GraphUtils.getElements(nextEdge));
                            GraphUtils.putValue(newEdge+':'+key,values);
                        }
                    } else {
                        GraphUtils.addValueToCollection(newEdge+':'+key,key);
                    }
                }
            } catch (ScriptException e) {
            System.out.println(e.getMessage());
        }
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }
}
