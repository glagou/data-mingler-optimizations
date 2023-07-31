package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.AggregationType;
import gr.aueb.data_mingler_optimizations.enumerator.KeyMode;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.exception.InvalidNumberOfCmdArgumentsException;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import javax.script.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class ThetaCombineOperator {
    private static final ScriptEngineManager manager = new ScriptEngineManager();
    private static final ScriptEngine engine = manager.getEngineByName("python");

    public static void run(String rootNode, String newChildNode, String allChildNodesCL, String outputChildNodesCL,
                            String thetaCL, KeyMode keyMode) throws InvalidNumberOfCmdArgumentsException {

        Instant start = Instant.now();
        boolean hasOutput = !outputChildNodesCL.isEmpty();
        String[] allChildNodes = allChildNodesCL.split(StringConstant.COMMA.getValue());
        String[] outputChildNodes = outputChildNodesCL.split(StringConstant.COMMA.getValue());

        String theta;
        if (!thetaCL.isEmpty()) {
            theta = thetaCL.replace('$'+rootNode+'$', "key");
            for (String childNode : allChildNodes) {
                theta = theta.replace(
                        '$'+childNode+'$', "r.lrange(\""+rootNode+'-'+childNode+":\"+key,0,1)[0]"
                );
            }
        }

        // TODO: Improve this
        boolean isFirst = true;
        Set<String> keys = new HashSet<>();
        for (String childNode : allChildNodes) {
            String edge = rootNode + "-" + childNode;
            if  (isFirst) {

                keys = (Set<String>) GraphUtils.getElements(edge);
                isFirst = false;
            }
            keys.retainAll((Set<String>) GraphUtils.getElements(edge));
        }

        String newEdge = rootNode + '-' + newChildNode;

        for (String key : keys) {
            // TODO: eval theta
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println(timeElapsed);
    }
}
