package gr.aueb.data_mingler_optimizations.exception;

public class EdgeNotFoundException extends RuntimeException {

    private static final String MESSAGE = "Datasource: %s was not found in the dictionary for the edge between nodes %s and %s";

    public EdgeNotFoundException(String datasource, String nodeA, String nodeB) {
        super(String.format(MESSAGE, datasource, nodeA, nodeB));
    }

}
