package gr.aueb.data_mingler_optimizations.enumerator;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum Output {

    YES("yes"),
    NO("no");

    final String value;

}
