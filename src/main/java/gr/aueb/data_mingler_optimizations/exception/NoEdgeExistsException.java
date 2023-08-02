package gr.aueb.data_mingler_optimizations.exception;

public class NoEdgeExistsException extends RuntimeException {

    private static final String MESSAGE = "There is no edge between %s and %s";

    public NoEdgeExistsException(String nodeA, String nodeB) {
        super(String.format(MESSAGE, nodeA, nodeB));
    }

}
