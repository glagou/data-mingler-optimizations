package gr.aueb.data_mingler_optimizations.util;

import jep.Interpreter;
import jep.JepException;
import jep.MainInterpreter;

import java.util.ArrayList;
import java.util.List;

public class PythonUtils {
    private static final List<String> importList = new ArrayList<>();

    public static void initialize(String jepPath) {
        MainInterpreter.setJepLibraryPath(jepPath);
    }

    public static void importOnceToInterpreter(String imports, Interpreter interpreter) throws JepException {
        if (!importList.contains(imports)) {
            interpreter.exec(imports);
            importList.add(imports);
        }
    }

    public static Object getValueFromScript(String script, Interpreter interpreter) throws JepException {
        String[] lines = script.split("\n");
        for (int i = 0; i < lines.length; i++) {
            interpreter.exec(lines[i]);
            if (i == lines.length - 1) {
                 return interpreter.getValue(lines[i]);
            }
        }
        return null;
    }

    public static boolean evalFromScript(String script, Interpreter interpreter) throws JepException {
        String[] lines = script.split("\n");
        for (int i = 0; i < lines.length; i++) {
            interpreter.exec(lines[i]);
            if (i == lines.length - 1) {
                return interpreter.eval(lines[i]);
            }
        }
        throw new RuntimeException("Something went wrong with the evaluation of the script");
    }
}
