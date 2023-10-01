package gr.aueb.data_mingler_optimizations.python;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Script {
    private final String[] script;

    public Script(String script) {
        if (isPath(script)) {
            this.script = readScriptFromFile(Path.of(script)).toArray(new String[0]);
        } else {
            this.script = script.split("\n");
        }
    }

    private boolean isPath(String script) {
        return script.endsWith(".py");
    }

    private List<String> readScriptFromFile(Path path) {
        List<String> lines;
        try {
            lines = Files.readAllLines(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return lines;
    }

    public String[] getScript() {
        return script;
    }

    public void renameScriptVariable(String oldName, String newName) {
        for (int i = 0; i < script.length; i++) {
            script[i] = script[i].replace(oldName, newName).trim();
        }
    }
}
