package gr.aueb.data_mingler_optimizations.enumerator;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum StringConstant {

    CHILD_OF_PREFIX("childOf", "The prefix of a child node that is used for symbolizing (creating) a " +
            "child of a node"),
    COMMA(",", "The comma character"),
    HYPHEN("-", "The hyphen character"),
    COLON(":", "The colon character"),
    SEMI_COLON(";", "The semi-colon character"),
    NULL("null", "The NULL value as a string"),
    EMPTY("", "The empty string");

    final String value;
    final String description;
}
