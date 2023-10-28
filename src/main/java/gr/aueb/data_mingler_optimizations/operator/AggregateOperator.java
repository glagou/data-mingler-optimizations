package gr.aueb.data_mingler_optimizations.operator;

import gr.aueb.data_mingler_optimizations.enumerator.AggregationType;
import gr.aueb.data_mingler_optimizations.enumerator.GraphAdditionMethod;
import gr.aueb.data_mingler_optimizations.enumerator.StringConstant;
import gr.aueb.data_mingler_optimizations.util.GraphUtils;

import java.util.*;

public class AggregateOperator {

    private static boolean isNotNumeric(String string) {
        try {
            Double.parseDouble(string);
            return false;
        } catch (NumberFormatException | NullPointerException e) {
            return true;
        }
    }

    private static boolean isNumericList(List<String> list) {
        for (String element : list) {
            if (isNotNumeric(element)) {
                return false;
            }
        }
        return true;
    }

    private static void calculateMin(String edge, Set<String> keys) {
        for (String key : keys) {
            String result;
            String graphKey = edge + ":" + key;
            List<String> values = (List<String>) GraphUtils.getElements(graphKey);
            if (values.size() != 0) {
                GraphUtils.removeElement(graphKey);
                if (isNumericList(values)) {
                    double min = Double.MAX_VALUE;
                    for (String value : values) {
                        double d = Double.parseDouble(value);
                        if (d < min) {
                            min = d;
                        }
                    }
                    result = String.valueOf(min);
                } else {
                    result = Collections.min(values);
                }
                GraphUtils.addValueToCollection(graphKey, result, GraphAdditionMethod.AS_LIST);
            } else {
                GraphUtils.addValueToCollection(graphKey, StringConstant.NULL.getValue(), GraphAdditionMethod.AS_LIST);
            }
        }
    }

    private static void calculateMax(String edge, Set<String> keys) {
        for (String key : keys) {
            String result;
            String graphKey = edge + ":" + key;
            List<String> values = (List<String>) GraphUtils.getElements(graphKey);
            if (values.size() != 0) {
                GraphUtils.removeElement(graphKey);
                if (isNumericList(values)) {
                    double max = Double.MIN_VALUE;
                    for (String value : values) {
                        double d = Double.parseDouble(value);
                        if (d > max) {
                            max = d;
                        }
                    }
                    result = String.valueOf(max);
                } else {
                    result = Collections.max(values);
                }
                GraphUtils.addValueToCollection(graphKey, result, GraphAdditionMethod.AS_LIST);
            } else {
                GraphUtils.addValueToCollection(graphKey, StringConstant.NULL.getValue(), GraphAdditionMethod.AS_LIST);
            }
        }
    }

    private static void calculateSum(String edge, Set<String> keys) {
        for (String key : keys) {
            String graphKey = edge + ":" + key;
            List<String> values = (List<String>) GraphUtils.getElements(graphKey);

            if (!(values == null) && isNumericList(values)) {
                double sum = 0;
                for (String value : values) {
                    double val = Double.parseDouble(value);
                    sum += val;
                }
                GraphUtils.removeElement(graphKey);
                GraphUtils.addValueToCollection(graphKey, String.valueOf(sum), GraphAdditionMethod.AS_LIST);
            } else {
                GraphUtils.addValueToCollection(graphKey, StringConstant.NULL.getValue(), GraphAdditionMethod.AS_LIST);
            }
        }
    }



    private static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }


    private static void calculateAverage(String edge, Set<String> keys) {
        for (String key : keys) {
            String result;
            String graphKey = edge + ":" + key;
            List<String> values = (List<String>) GraphUtils.getElements(graphKey);
            if (values.size() != 0) {
                GraphUtils.removeElement(graphKey);
                boolean foundAtLeastOne = false;
                double sum = 0;
                int cnt = 0;
                for (String value : values) {
                    if (isNotNumeric(value)) {
                        continue;
                    }
                    double val = Double.parseDouble(value);
                    foundAtLeastOne = true;
                    sum = sum + val;
                    cnt++;
                }
                if (foundAtLeastOne) {
                    result = String.valueOf(sum / cnt);
                    GraphUtils.addValueToCollection(graphKey, result, GraphAdditionMethod.AS_LIST);
                } else {
                    GraphUtils.addValueToCollection(graphKey, StringConstant.NULL.getValue(), GraphAdditionMethod.AS_LIST);
                }
            } else {
                GraphUtils.addValueToCollection(graphKey, StringConstant.NULL.getValue(), GraphAdditionMethod.AS_LIST);
            }
        }
    }

    private static void calculateCount(String edge, Set<String> keys) {
        for (String key : keys) {
            String result;
            String graphKey = edge + ":" + key;
            long size = GraphUtils.getElements(graphKey).size();
            GraphUtils.removeElement(graphKey);
            result = String.valueOf(size);
            GraphUtils.addValueToCollection(graphKey, result, GraphAdditionMethod.AS_LIST);
        }
    }

    private static void calculateAny(String edge, Set<String> keys) {
        for (String key : keys) {
            String result;
            String graphKey = edge + ":" + key;
            List<String> values = (List<String>) GraphUtils.getElements(graphKey);
            if (values.size() != 0) {
                GraphUtils.removeElement(graphKey);
                result = values.get(0);
                GraphUtils.addValueToCollection(graphKey, result, GraphAdditionMethod.AS_LIST);
            }
        }
    }

    public static void run(String rootNode, String childNode, AggregationType aggregationType) {
        String edge = rootNode + "-" + childNode;
        Set<String> keys = (Set<String>) GraphUtils.getElements(edge);
        switch (aggregationType) {
            case MIN -> calculateMin(edge, keys);
            case MAX -> calculateMax(edge, keys);
            case SUM -> calculateSum(edge, keys);
            case AVERAGE -> calculateAverage(edge, keys);
            case COUNT -> calculateCount(edge, keys);
            case ANY -> calculateAny(edge, keys);
        }
    }

}
