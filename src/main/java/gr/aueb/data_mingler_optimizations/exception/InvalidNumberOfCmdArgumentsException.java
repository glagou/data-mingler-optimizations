package gr.aueb.data_mingler_optimizations.exception;

public class InvalidNumberOfCmdArgumentsException extends RuntimeException {

    private static final String MESSAGE = "The number of command line arguments provided is invalid";

    public InvalidNumberOfCmdArgumentsException() {
        super(MESSAGE);
    }

}
