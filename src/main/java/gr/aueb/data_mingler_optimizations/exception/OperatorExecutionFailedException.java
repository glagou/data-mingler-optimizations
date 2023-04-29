package gr.aueb.data_mingler_optimizations.exception;

import gr.aueb.data_mingler_optimizations.enums.Operator;

public class OperatorExecutionFailedException extends RuntimeException {

    private static final String MESSAGE = "Execution of gr.aueb.data_mingler_optimizations.operator with type: %s has failed";

    public OperatorExecutionFailedException(Operator operator) {
        super(String.format(MESSAGE, operator.name()));
    }
}
