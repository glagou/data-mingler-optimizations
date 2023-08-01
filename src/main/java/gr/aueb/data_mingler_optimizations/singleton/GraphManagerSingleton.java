package gr.aueb.data_mingler_optimizations.singleton;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GraphManagerSingleton {

    private static final Map<String, Collection<String>> GRAPH = new HashMap<>();

    private GraphManagerSingleton() {
        // Preventing instantiation through a private constructor
    }

    public static Map<String, Collection<String>> getGraph() {
        return GRAPH;
    }

}
