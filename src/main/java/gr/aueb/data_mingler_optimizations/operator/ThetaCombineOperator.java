package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.python.Script;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;
import gr.aueb.data_mingler_optimizations.util.PythonUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ThetaCombineOperator {

    private static CompletableFuture<Boolean> evaluateTheta(String theta, String key, String rootNode, String[] allChildNodes) {
        if (theta.equalsIgnoreCase("true")) return CompletableFuture.completedFuture(true);

        return PythonUtils.executePython(() -> {
                    Script pythonCode = new Script(theta);
                    for (String childNode : allChildNodes) {
                        String element = ((List<String>) GraphUtils.getElements(rootNode + '-' + childNode + ':' + key)).get(0);
                        pythonCode.renameScriptVariable('$' + childNode + '$', element);
                    }
                    pythonCode.createVariable("key", key);
                    return PythonUtils.createPythonProcess(pythonCode);
                })
                .thenApply(result -> result != null && result.equalsIgnoreCase("true"));
    }

    public static void run(String rootNode, String newChildNode, String allChildNodesCL, String outputChildNodesCL,
                           String thetaCL) {
        Instant start = Instant.now();

        boolean hasOutput = !outputChildNodesCL.isEmpty();
        String[] allChildNodes = allChildNodesCL.split(StringConstant.COMMA.getValue());
        String[] outputChildNodes = outputChildNodesCL.split(StringConstant.COMMA.getValue());

        String theta = "True";
        if (!thetaCL.isEmpty()) {
            theta = thetaCL.replace('$' + rootNode + '$', "key");
        }

        boolean isFirst = true;
        Set<String> keys = new HashSet<>();
        for (String childNode : allChildNodes) {
            if (isFirst) {
                keys = (Set<String>) GraphUtils.getElements(rootNode + "-" + childNode);
                isFirst = false;
                continue;
            }
            keys.retainAll(GraphUtils.getElements(rootNode + "-" + childNode));
        }

        String newEdge = rootNode + '-' + newChildNode;

        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (String key : keys) {
            CompletableFuture<Void> future = evaluateTheta(theta, key, rootNode, allChildNodes).thenAccept(result -> {
                if (result) {
                    GraphUtils.addValueToCollection(newEdge, key, GraphAdditionMethod.AS_SET);
                    if (hasOutput) {
                        for (String childNode : outputChildNodes) {
                            String nextEdge = rootNode + '-' + childNode + ':' + key;
                            Collection<String> elements = GraphUtils.getElements(nextEdge);
                            if (elements != null) {
                                List<String> values = new ArrayList<>(elements);
                                GraphUtils.setCollection(newEdge + ':' + key, values);
                            }
                        }
                    } else {
                        GraphUtils.addValueToCollection(newEdge + ':' + key, key, GraphAdditionMethod.AS_SET);
                    }
                }
            });
            futureList.add(future);
        }
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }
}
