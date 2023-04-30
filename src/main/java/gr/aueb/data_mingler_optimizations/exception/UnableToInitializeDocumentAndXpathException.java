package gr.aueb.data_mingler_optimizations.exception;

public class UnableToInitializeDocumentAndXpathException extends RuntimeException {

    private static final String MESSAGE = "Document and Xpath could not be initialized with queryFilename: %s";

    public UnableToInitializeDocumentAndXpathException(String queryFilename) {
        super(String.format(MESSAGE, queryFilename));
    }

}
