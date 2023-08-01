package gr.aueb.data_mingler_optimizations.exception;

public class LoadEdgesExecutionFailedException extends RuntimeException {

    private static final String MESSAGE = "EdgesLoader program execution failed for root node: %s";

    public LoadEdgesExecutionFailedException(String rootNode, Exception e) {
        super(String.format(MESSAGE, rootNode), e);
    }

}
