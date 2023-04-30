package gr.aueb.data_mingler_optimizations.exception;

public class InvalidNumberOfCmdArgumentsException extends RuntimeException {

    private static final String MESSAGE = "Three command line arguments should be provided";

    public InvalidNumberOfCmdArgumentsException() {
        super(MESSAGE);
    }

}
