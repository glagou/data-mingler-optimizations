package gr.aueb.data_mingler_optimizations.exception;

public class MissingEnvironmentVariableException extends RuntimeException {

    private static final String MESSAGE = "Missing environment variable %s";

    public MissingEnvironmentVariableException(String envVar) {
        super(String.format(MESSAGE, envVar));
    }

}
