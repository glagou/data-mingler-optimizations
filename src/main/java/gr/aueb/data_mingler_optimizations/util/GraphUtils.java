package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.QueryEvaluation;
import gr.aueb.data_mingler_optimizations.enumerator.KeyMode;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.graph.GraphManagerSingleton;

import java.util.*;

public class GraphUtils {

    private static final Map<String, Collection<String>> GRAPH = GraphManagerSingleton.getGraph();

    private static String createGraphKeyWithHyphen(String rootNode, String childNode) {
        return rootNode
                .concat(StringConstant.HYPHEN.getValue())
                .concat(childNode);
    }

    public static void removeEdge(String rootNode, String childNode) {
        String baseKey = createGraphKeyWithHyphen(rootNode, childNode);
        Collection<String> keys = GRAPH.get(baseKey);
        keys.forEach(key -> {
            String childKey = baseKey
                    .concat(StringConstant.COMMA.getValue())
                    .concat(key);
            GRAPH.remove(childKey);
        });
        GRAPH.remove(baseKey);
    }

    public static Collection<String> combineKeys(String rootNode, List<String> childNodes) {
        String graphKey = createGraphKeyWithHyphen(rootNode, childNodes.get(0));
        Collection<String> keys = GRAPH.get(graphKey);
        childNodes.forEach(childNode -> {
            String childNodeGraphKey = createGraphKeyWithHyphen(rootNode, childNode);
            if (QueryEvaluation.getKeysMode() == KeyMode.ALL) {
                keys.addAll(GRAPH.get(childNodeGraphKey));
            } else {
                keys.retainAll(GRAPH.get(childNodeGraphKey));
            }
        });
        return keys;
    }

    public static Collection<String> getElements(String edge) {
        return GRAPH.get(edge);
    }

    public static int getNumberOfElementsWithHyphen(String rootNode, String childNode) {
        String key = createGraphKeyWithHyphen(rootNode, childNode);
        return GRAPH.get(key) != null ? GRAPH.get(key).size() : 0;
    }

    public static void setCollection(String key, Collection<String> value) {
        GRAPH.put(key, value);
    }

    public static void addValueToCollection(String key, String value) {
        Collection<String> initialCollection = GRAPH.get(key);
        if (initialCollection == null) {
            initialCollection = new HashSet<>();
        }
        initialCollection.add(value);
        GRAPH.put(key, initialCollection);
    }

    public static void removeElement(String key) {
        GRAPH.remove(key);
    }

}
