package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import java.util.List;
import java.util.Set;


public class RollupOperator {

    public static void run(String rootNode, String childNode, String childOfChildNode) {
        String edge1 = rootNode + "-" + childNode;
        String edge2 = childNode + "-" + childOfChildNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge1);
        for (String key : keys) {
            String graphKey = edge1 + ":" + key;
            List<String> values = (List<String>) GraphUtils.getElements(graphKey);
            GraphUtils.removeElement(graphKey);
            for (String value : values) {
                String graphKey2 = edge2 + ":" + value;
                List<String> values2 = (List<String>) GraphUtils.getElements(graphKey2);
                if (values2 != null) {
                    for (String value2 : values2) {
                        GraphUtils.addValueToCollection(graphKey, value2, GraphAdditionMethod.AS_LIST);
                    }
                }
            }
        }
    }

}
