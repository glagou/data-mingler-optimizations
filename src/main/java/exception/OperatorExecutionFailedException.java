package exception;

import enums.Operator;

public class OperatorExecutionFailedException extends RuntimeException {

    private static final String MESSAGE = "Execution of operator with type: %s has failed";

    public OperatorExecutionFailedException(Operator operator) {
        super(String.format(MESSAGE, operator.name()));
    }
}
