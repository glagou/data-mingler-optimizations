package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.python.Script;
import jep.Interpreter;
import jep.JepException;

public class PythonUtils {
    public static Object getValueFromScript(Interpreter interpreter, Script pythonCode) {
        return executePython(interpreter, pythonCode);
    }

    public static boolean evalFromScript(Interpreter interpreter, Script pythonCode) {
        Object obj = executePython(interpreter, pythonCode);
        if (obj instanceof Boolean) {
            return (boolean) obj;
        }
        throw new RuntimeException("Python script did not return boolean");
    }

    private static Object executePython(Interpreter interpreter, Script pythonCode) {
        String[] lines = pythonCode.getScript();
        try {
            for (int i = 0; i < lines.length - 1; i++) {
                String line = lines[i];
                if (!line.trim().isEmpty()) {
                    interpreter.exec(line);
                }
            }
            String lastLine = lines[lines.length - 1];
            return interpreter.getValue(lastLine);
        } catch (JepException e) {
            throw new RuntimeException(e);
        }
    }


}
