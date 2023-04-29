package exception;

public class TransformationsAreInvalidException extends RuntimeException {

    private static final String MESSAGE = "Transformations cannot be null or empty";

    public TransformationsAreInvalidException() {
        super(MESSAGE);
    }

}
