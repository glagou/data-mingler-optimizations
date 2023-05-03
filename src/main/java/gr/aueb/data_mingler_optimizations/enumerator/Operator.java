package gr.aueb.data_mingler_optimizations.enumerator;

public enum Operator {

    AGGREGATE("aggregate"),
    FILTER("filter"),
    MAP("map"),
    THETA_COMBINE("thetaCombine"),
    ROLLUP_COMBINE("rollupCombine");

    private final String nameAsCommandLineArgument;


    Operator(String nameAsCommandLineArgument) {
        this.nameAsCommandLineArgument = nameAsCommandLineArgument;
    }

    public String getNameAsCommandLineArgument() {
        return nameAsCommandLineArgument;
    }
}
