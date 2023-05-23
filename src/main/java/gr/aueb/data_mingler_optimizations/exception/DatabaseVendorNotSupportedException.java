package gr.aueb.data_mingler_optimizations.exception;

import gr.aueb.data_mingler_optimizations.enumerator.DatabaseType;

public class DatabaseVendorNotSupportedException extends RuntimeException {

    private static final String MESSAGE = "Database vendor not supported yet: %s";

    public DatabaseVendorNotSupportedException(DatabaseType dbSystem) {
        super(String.format(MESSAGE, dbSystem.toString()));
    }

}
