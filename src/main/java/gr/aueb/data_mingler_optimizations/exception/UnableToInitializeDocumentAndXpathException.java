package gr.aueb.data_mingler_optimizations.exception;

public class UnableToInitializeDocumentAndXpathException extends RuntimeException {

    private static final String MESSAGE = "Document and Xpath could not be initialized for file name: %s";

    public UnableToInitializeDocumentAndXpathException(String fileName) {
        super(String.format(MESSAGE, fileName));
    }

}
