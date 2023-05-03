package gr.aueb.data_mingler_optimizations.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GraphManagerSingleton {

    // TODO: Verify this is a Set<String>
    private static final Map<String, Set<String>> GRAPH = new HashMap<>();

    // Preventing instantiation through a private constructor
    private GraphManagerSingleton() {}

    public static Map<String, Set<String>> getGraph() {
        return GRAPH;
    }

}
