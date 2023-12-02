package gr.aueb.data_mingler_optimizations.python;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class Script {
    private String script;

    public Script(String script) {
        if (isPath(script)) {
            this.script = readScriptFromFile(Path.of(script));
        } else {
            this.script = script;
        }
    }

    private boolean isPath(String script) {
        return script.endsWith(".py");
    }

    private String readScriptFromFile(Path path) {
        List<String> lines;
        try {
            lines = Files.readAllLines(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        lines = lines.stream()
                .filter(line -> !line.trim().isEmpty())
                .collect(Collectors.toList());
        return String.join(";", lines);
    }

    public String getScript() {
        return script;
    }

    public void renameScriptVariable(String oldName, String newName) {
        this.script = script.replace(oldName, newName);
    }

    public void createVariable(String name, String value) {
        this.script = name + " = " + value + ";" + script;
    }
}
