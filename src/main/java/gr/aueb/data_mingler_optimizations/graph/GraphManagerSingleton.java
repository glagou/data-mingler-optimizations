package gr.aueb.data_mingler_optimizations.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GraphManagerSingleton {

    private static final GraphManagerSingleton INSTANCE = new GraphManagerSingleton();

    // TODO: Verify types (String, Object)
    private static final Map<String, Set<String>> GRAPH = new HashMap<>();

    // Preventing instantiation through a private constructor
    private GraphManagerSingleton() {}

    public static GraphManagerSingleton getInstance() {
        return INSTANCE;
    }

    public static Map<String, Set<String>> getGraph() {
        return GRAPH;
    }

}
