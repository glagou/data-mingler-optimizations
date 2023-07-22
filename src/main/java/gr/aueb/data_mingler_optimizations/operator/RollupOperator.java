package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class RollupOperator {

    private static void validateCmdArguments(String[] args) {
        if (args.length != 3) {
            throw new InvalidNumberOfCmdArgumentsException();
        }
    }

    public static void main(String[] args) throws InvalidNumberOfCmdArgumentsException {
        validateCmdArguments(args);

        String rootNode = args[0];
        String childNode = args[1];
        String childChildNode = args[2];

        Instant start = Instant.now();

        String edge1 = rootNode + "-" + childNode;
        String edge2 = childNode + "-" + childChildNode;

        Set<String> keys = new HashSet<>(GraphUtils.getElements(edge1));
        for (String key : keys) {
            List<String> values = new ArrayList<>(GraphUtils.getElements(edge1 + ":" + key));
            GraphUtils.removeElement(edge1 + ":" + key);
            for (String value : values) {
                List<String> values2 = new ArrayList<>(GraphUtils.getElements(edge2 + ":" + value));
                GraphUtils.putValue(edge1 + ":" + key, values2);
            }
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }
}
