package gr.aueb.data_mingler_optimizations.singleton;

import jep.Interpreter;
import jep.JepException;
import jep.MainInterpreter;
import jep.SharedInterpreter;

import java.util.ArrayList;
import java.util.List;

public class PythonManager {
    private static final List<String> importList = new ArrayList<>();

    public static void initialize(String jepPath) {
        MainInterpreter.setJepLibraryPath(jepPath);
    }

    public static void importOnce(String imports) {
        try (Interpreter interpreter = new SharedInterpreter()) {
            if (!importList.contains(imports)) {
                interpreter.exec(imports);
                importList.add(imports);
            }
        } catch (JepException e) {
            throw new RuntimeException(e);
        }
    }
}
