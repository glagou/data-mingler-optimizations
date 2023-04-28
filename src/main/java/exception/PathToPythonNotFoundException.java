package exception;

public class PathToPythonNotFoundException extends RuntimeException {

    private static final String MESSAGE = "Path to python could not be found from PATH environment variable";

    public PathToPythonNotFoundException() {
        super(MESSAGE);
    }

}
