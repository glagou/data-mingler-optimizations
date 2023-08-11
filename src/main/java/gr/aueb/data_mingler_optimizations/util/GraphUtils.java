package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.QueryEvaluation;
import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.enumerator.KeysMode;
import gr.aueb.data_mingler_optimizations.singleton.GraphManagerSingleton;

import java.util.*;

public class GraphUtils {

    private static final Map<String, Collection<String>> GRAPH = GraphManagerSingleton.getGraph();

    public static Collection<String> combineKeys(String rootNode, List<String> childNodes) {
        Set<String> keys = null;
        boolean isFirst = true;
        for (String childNode : childNodes) {
            String graphKey = rootNode + "-" + childNode;
            if (isFirst) {
                keys = (Set<String>) GraphUtils.getElements(graphKey);
                isFirst = false;
                continue;
            }
            if (QueryEvaluation.getKeysMode() == KeysMode.ALL)
                keys.addAll(GRAPH.get(graphKey));
            else {
                keys.retainAll(GRAPH.get(graphKey));
            }
        }
        return keys;
    }

    public static Collection<String> getElements(String edge) {
        return GRAPH.get(edge);
    }

    public static void addValueToCollection(String key, String value, GraphAdditionMethod graphAdditionMethod) {
        Collection<String> initialCollection = GRAPH.get(key);
        if (initialCollection == null) {
            if (graphAdditionMethod == GraphAdditionMethod.AS_SET) {
                initialCollection = new HashSet<>();
            } else {
                initialCollection = new ArrayList<>();
            }
        }
        initialCollection.add(value);
        GRAPH.put(key, initialCollection);
    }

    public static void setCollection(String key, Collection<String> values) {
        GRAPH.put(key, values);
    }

    public static void removeElement(String key) {
        GRAPH.remove(key);
    }

}
