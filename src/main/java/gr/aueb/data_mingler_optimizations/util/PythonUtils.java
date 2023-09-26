package gr.aueb.data_mingler_optimizations.util;

import jep.Interpreter;
import jep.JepException;
import jep.MainInterpreter;
import jep.SharedInterpreter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PythonUtils {
    private static final List<String> importList = new ArrayList<>();
    private static final Pattern importPattern = Pattern.compile("^import .*|^from .* import .*");
    private static final Interpreter interpreter = new SharedInterpreter(); // TODO: place multiple shared interpreters in operators

    public static Interpreter getInterpreter() {
        return interpreter;
    }

    public static Object getValueFromScript(String script) {
        return executePython(script);
    }

    public static boolean evalFromScript(String script) {
        Object obj = executePython(script);
        if (obj instanceof Boolean) {
            return (boolean) obj;
        }
        throw new RuntimeException("Python script did not return boolean");
    }

    private static Object executePython(String script) {
        if (!script.endsWith(".py")) {
            String[] lines = script.split("\n");
            for (int i = 0; i < lines.length; i++) {
                interpreter.exec(lines[i].trim());
                if (i == lines.length - 1) {
                    return interpreter.getValue(lines[i].trim());
                }

            }
        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader(script));
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = importPattern.matcher(line);
                if (matcher.matches() && !importList.contains(line)) {
                    importList.add(line);
                    interpreter.exec(line);
                    continue;
                }
                if (!line.trim().isEmpty()) {
                    String nextLine = reader.readLine();
                    if (nextLine == null) {
                        return interpreter.getValue(line);
                    }
                    interpreter.exec(line);
                }

            }
        } catch (IOException | JepException e) {
            throw new RuntimeException(e);
        }
        return null;
    }


}
