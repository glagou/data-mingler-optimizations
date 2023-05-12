package gr.aueb.data_mingler_optimizations.graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GraphManagerSingleton {

    // TODO: Verify this is a Collection<String>
    private static final Map<String, Collection<String>> GRAPH = new HashMap<>();

    // Preventing instantiation through a private constructor
    private GraphManagerSingleton() {}

    public static Map<String, Collection<String>> getGraph() {
        return GRAPH;
    }

}
