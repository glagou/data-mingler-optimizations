package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.singleton.PythonInterpreterSingleton;
import jep.JepException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PythonUtils {
    private static final Pattern importPattern = Pattern.compile("^import .*|^from .* import .*");

    public static Object getValueFromScript(String[] lines) {
        return executePython(lines);
    }

    public static boolean evalFromScript(String[] lines) {
        Object obj = executePython(lines);
        if (obj instanceof Boolean) {
            return (boolean) obj;
        }
        throw new RuntimeException("Python script did not return boolean");
    }

    private static Object executePython(String[] lines) {
        try {
            for (int i = 0; i < lines.length - 1; i++) {
                String line = lines[i];
                Matcher matcher = importPattern.matcher(line);
                if (matcher.matches()) {
                    if (!PythonInterpreterSingleton.getImportList().contains(line)) {
                        PythonInterpreterSingleton.getImportList().add(line);
                        PythonInterpreterSingleton.getInterpreter().exec(line);
                    }
                    continue;
                }
                if (!line.trim().isEmpty()) {
                    PythonInterpreterSingleton.getInterpreter().exec(line);
                }
            }
            String lastLine = lines[lines.length - 1];
            return PythonInterpreterSingleton.getInterpreter().getValue(lastLine);
        } catch (JepException e) {
            throw new RuntimeException(e);
        }
    }


}
