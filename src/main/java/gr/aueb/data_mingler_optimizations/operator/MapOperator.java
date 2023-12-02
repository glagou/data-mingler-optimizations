package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.python.Script;
import gr.aueb.data_mingler_optimizations.util.PythonUtils;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class MapOperator {

    public static void run(String rootNode, String childNode, String script) {
        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);

        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            Collection<String> values = GraphUtils.getElements(graphKey);
            GraphUtils.removeElement(graphKey);
            if (values == null) continue;
            for (String value : values) {
                CompletableFuture<Void> future = PythonUtils.executePython(() -> {
                        Script pythonCode = new Script(script);
                        pythonCode.renameScriptVariable('$' + childNode + '$', "value");
                        pythonCode.createVariable("value", value);
                        return PythonUtils.createPythonProcess(pythonCode);
                    }
                ).thenAccept(result -> {
                    if (result != null && !result.toLowerCase().contains("e") && result.matches("-?\\d+(\\.\\d+)?")) {
                        GraphUtils.addValueToCollection(graphKey, result, GraphAdditionMethod.AS_LIST);
                    }
                });
                futureList.add(future);
            }
        }
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
    }
}
