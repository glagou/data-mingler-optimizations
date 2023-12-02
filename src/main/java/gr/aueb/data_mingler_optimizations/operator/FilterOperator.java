package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.python.Script;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import gr.aueb.data_mingler_optimizations.util.PythonUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class FilterOperator {

    public static void run(String rootNode, String childNode, String expressionCL) {
        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            Collection<String> values = GraphUtils.getElements(graphKey);
            GraphUtils.removeElement(graphKey);
            for (String value : values) {
                String value2;
                if (value.matches("-?\\d+(\\.\\d+)?")) {
                    value2 = String.valueOf(Double.parseDouble(value));
                } else {
                    value2 = value;
                }

                CompletableFuture<Void> future = PythonUtils.executePython(() -> {
                    Script pythonCode = new Script(expressionCL);
                    pythonCode.renameScriptVariable('$' + childNode + '$', "Lvalue");
                    pythonCode.createVariable("Lvalue", value2);
                    return PythonUtils.createPythonProcess(pythonCode);
                }).thenAccept(result -> {
                    if (result != null && result.equalsIgnoreCase("true")) {
                        GraphUtils.addValueToCollection(graphKey, value, GraphAdditionMethod.AS_LIST);
                    }
                });
                futureList.add(future);
            }
        }

        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
    }
}
