package util.graph;

import enums.KeyMode;
import manager.graph.GraphManagerSingleton;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphUtils {

    private static final Map<String, Set<String>> GRAPH = GraphManagerSingleton.getGraph();
    private static final String HYPHEN = "-";

    private static String createGraphKey(String rootNode, String childNode) {
        return rootNode
                .concat(HYPHEN)
                .concat(childNode);
    }

    public static void removeEdge(String rootNode, String childNode) {
        String key = createGraphKey(rootNode, childNode);
        GRAPH.remove(key);
    }

    public static Set<String> combineKeys(String rootNode, List<String> childNodes, KeyMode keyMode) {
        String graphKey = createGraphKey(rootNode, childNodes.get(0));
        Set<String> keys = GRAPH.get(graphKey);
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
