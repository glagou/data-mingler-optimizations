package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import org.python.jsr223.PyScriptEngineFactory;

import javax.script.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ThetaCombineOperator {
    private static final ScriptEngineManager manager = new ScriptEngineManager();

    public static void run(String rootNode, String newChildNode, String allChildNodesArg, String outputChildNodesArg,
                           String thetaArg) {

        manager.registerEngineExtension("python", new PyScriptEngineFactory());
        ScriptEngine engine = manager.getEngineByName("python");

        String[] allChildNodes = allChildNodesArg.split(StringConstant.COMMA.getValue());
        String[] outputChildNodes = outputChildNodesArg.split(StringConstant.COMMA.getValue());

        String theta = "True";
        if (!thetaArg.isEmpty()) {
            theta = thetaArg.replace('$' + rootNode + '$', "key");
            for (String childNode : allChildNodes) {
                theta = thetaArg.replace("$" + childNode + "$",
                        "r.lrange(\"" + rootNode + "-" + childNode + ":\"+key,0,1)[0]");
            }
        }

        boolean isFirst = true;
        Set<String> keys = new HashSet<>();
        for (String childNode : allChildNodes) {
            String graphKey = rootNode + "-" + childNode;
            if (isFirst) {
                keys = (Set<String>) GraphUtils.getElements(graphKey);
                isFirst = false;
                continue;
            }
            keys.retainAll(GraphUtils.getElements(graphKey));
        }

        String newEdge = rootNode + '-' + newChildNode;
        for (String key : keys) {
            try {
                Bindings bindings = new SimpleBindings();
                bindings.put("key", key);
                Object result = engine.eval(theta, bindings);
                if (result instanceof Boolean && (Boolean) result) {
                    GraphUtils.addValueToCollection(newEdge, key, GraphAdditionMethod.AS_SET);
                    String graphKey = newEdge + ":" + key;
                    if (!outputChildNodesArg.isEmpty()) {
                        for (String childNode : outputChildNodes) {
                            String nextEdge = rootNode + '-' + childNode + ':' + key;
                            Collection<String> values = GraphUtils.getElements(nextEdge);
                            for (String value : values) {
                                GraphUtils.addValueToCollection(graphKey, value, GraphAdditionMethod.AS_LIST);
                            }
                        }
                    } else {
                        GraphUtils.addValueToCollection(graphKey, key, GraphAdditionMethod.AS_LIST);
                    }
                }
            } catch (ScriptException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
