package gr.aueb.data_mingler_optimizations.singleton;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GraphManagerSingleton {

    private static final ConcurrentMap<String, Collection<String>> GRAPH = new ConcurrentHashMap<>();

    private GraphManagerSingleton() {
        // Preventing instantiation through a private constructor
    }

    public static ConcurrentMap<String, Collection<String>> getGraph() {
        return GRAPH;
    }

}
