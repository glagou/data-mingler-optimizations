package gr.aueb.data_mingler_optimizations.singleton;

import jep.Interpreter;
import jep.SharedInterpreter;

import java.util.ArrayList;
import java.util.List;

public class PythonInterpreterSingleton {

    private static final List<String> importList = new ArrayList<>();
    private static final Interpreter INTERPRETER = new SharedInterpreter();

    private PythonInterpreterSingleton() {
        // Preventing instantiation through a private constructor
    }

    public static Interpreter getInterpreter() {
        return INTERPRETER;
    }

    public static List<String> getImportList() {
        return importList;
    }
}
