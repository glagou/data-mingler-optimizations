
package gr.aueb.data_mingler_optimizations.exception;

public class UnableToConnectToDatabaseException extends RuntimeException {

    private static final String MESSAGE = """
            Unable to connect to database.
            Please check the following:
            1. The database is up and running.
            2. The database vendor is supported.
            3. The database credentials are correct.
            """;

    public UnableToConnectToDatabaseException() {
        super(MESSAGE);
    }

}
