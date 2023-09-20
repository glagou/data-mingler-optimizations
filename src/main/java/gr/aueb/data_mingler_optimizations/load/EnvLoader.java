package gr.aueb.data_mingler_optimizations.load;

import gr.aueb.data_mingler_optimizations.exception.MissingEnvironmentVariableException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class EnvLoader {
    private static final String ENV_FILE_PATH = ".env";
    private static final String[] requiredVariables = {"JEP_PATH"};

    public static void load() {
        if (envFileExists()) {
            loadEnvFile();
        }
        checkEnvVars();
    }

    private static boolean envFileExists() {
        Path path = Paths.get(ENV_FILE_PATH);
        return Files.exists(path) && !Files.isDirectory(path);
    }

    private static void checkEnvVars() {
        for (String var : requiredVariables) {
            if (System.getProperty(var) == null) {
                throw new MissingEnvironmentVariableException(var);
            }
        }
    }

    private static void loadEnvFile() {
        try (BufferedReader reader = new BufferedReader(new FileReader(ENV_FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    String key = parts[0].trim();
                    String value = parts[1].trim();
                    System.setProperty(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
