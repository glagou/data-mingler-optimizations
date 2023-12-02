package gr.aueb.data_mingler_optimizations.util;

import gr.aueb.data_mingler_optimizations.python.Script;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.*;

public class PythonUtils {
    private static final int numberOfCores = Runtime.getRuntime().availableProcessors();
    private static final ExecutorService executor = Executors.newFixedThreadPool(numberOfCores);

    public static CompletableFuture<String> executePython(Script pythonCode) {
        return CompletableFuture.supplyAsync(() -> createPythonProcess(pythonCode), executor);
    }

    private static String createPythonProcess(Script pythonCode) {
        String code = pythonCode.getScript();
        ProcessBuilder processBuilder = new ProcessBuilder("python3", "-c", code);
        try {
            Process process = processBuilder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            String lastStoredLine = null;
            while ((line = reader.readLine()) != null) {
                lastStoredLine = line;
            }
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                return lastStoredLine;
            } else {
                throw new RuntimeException("Python script exited with code " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
