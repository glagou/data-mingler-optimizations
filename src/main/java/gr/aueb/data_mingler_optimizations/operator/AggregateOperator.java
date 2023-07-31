package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.AggregationType;
import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class AggregateOperator {

    public static void run(String rootNode, String childNode,AggregationType aggrType) throws InvalidNumberOfCmdArgumentsException {
        Instant start = Instant.now();

        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        for (String key : keys) {
            Set<String> resultSet = new HashSet<>();
            Collection<String> values = GraphUtils.getElements(edge);
            GraphUtils.removeElement(edge.concat(":").concat(key));
            // TODO: should we add "null" like previously?
            switch (aggrType) {
                case MIN:
                    resultSet.add(Collections.min(values));
                    break;
                case MAX:
                    resultSet.add(Collections.max(values));
                    break;
                case SUM:
                    double sum = values.stream()
                            .filter((s) -> s.matches("\\d+(\\.\\d+)?"))
                            .mapToDouble(Double::parseDouble)
                            .sum();
                    resultSet.add(Double.toString(sum));
                    break;
                case AVERAGE:
                    sum = values.stream()
                            .filter((s) -> s.matches("\\d+(\\.\\d+)?"))
                            .mapToDouble(Double::parseDouble)
                            .sum();
                    long count = values.stream()
                            .filter((s) -> s.matches("\\d+(\\.\\d+)?"))
                            .count();
                    double avg = (count > 0) ? sum / count : 0.0;
                    resultSet.add(String.valueOf(avg));
                    break;
                case COUNT:
                    resultSet.add(String.valueOf(values.size()));
                    break;
                case ANY:
                    resultSet.add(new ArrayList<>(values).get(0));
                    break;
            }
            GraphUtils.putValue(edge.concat(":").concat(key), resultSet);
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }
}
