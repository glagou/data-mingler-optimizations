package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.enumerator.KeyMode;
import gr.aueb.data_mingler_optimizations.graph.GraphManagerSingleton;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphUtils {

    // TODO: Verify this is a Collection<String>
    private static final Map<String, Collection<String>> GRAPH = GraphManagerSingleton.getGraph();

    private static final String HYPHEN = "-";
    private static final String COLON = ":";

    private static String createGraphKey(String rootNode, String childNode) {
        return rootNode
                .concat(HYPHEN)
                .concat(childNode);
    }

    public static void putValue(String key, Collection<String> value) {
        GRAPH.put(key, value);
    }

    // TODO: Discuss why keys are stored this way and maybe refactor
    public static void removeEdge(String rootNode, String childNode) {
        String baseKey = createGraphKey(rootNode, childNode);
        Collection<String> keys = GRAPH.get(baseKey);
        keys.forEach(key -> {
            String childKey = baseKey
                    .concat(COLON)
                    .concat(key);
            GRAPH.remove(childKey);
        });
        GRAPH.remove(baseKey);
    }

    public static void removeElement(String key) {
        GRAPH.remove(key);
    }

    public static int getNumberOfElements(String rootNode, String childNode) {
        String key = createGraphKey(rootNode, childNode);
        return GRAPH.get(key).size();
    }

    public static Collection<String> getElements(String edge) {
        return GRAPH.get(edge);
    }

    public static Collection<String> combineKeys(String rootNode, List<String> childNodes, KeyMode keyMode) {
        String graphKey = createGraphKey(rootNode, childNodes.get(0));
        Collection<String> keys = GRAPH.get(graphKey);
        childNodes.forEach(childNode -> {
            String childNodeGraphKey = createGraphKey(rootNode, childNode);
            if (keyMode == KeyMode.ALL) {
                keys.addAll(GRAPH.get(childNodeGraphKey));
            } else {
                keys.retainAll(GRAPH.get(childNodeGraphKey));
            }
        });
        return keys;
    }

}
