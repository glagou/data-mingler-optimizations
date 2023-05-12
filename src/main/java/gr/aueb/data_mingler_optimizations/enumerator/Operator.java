package gr.aueb.data_mingler_optimizations.enumerator;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum Operator {

    AGGREGATE("aggregate"),
    FILTER("filter"),
    MAP("map"),
    THETA_COMBINE("thetaCombine"),
    ROLLUP_COMBINE("rollupCombine");

    private final String nameAsCommandLineArgument;

}
