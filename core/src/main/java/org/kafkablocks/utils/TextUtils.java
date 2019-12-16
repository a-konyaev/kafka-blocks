package org.kafkablocks.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.StreamSupport;


public final class TextUtils {
    private TextUtils() {
    }

    public static <T> String joinToString(T[] array) {
        return joinToString(Arrays.asList(array));
    }

    public static <T> String joinToString(Iterable<T> elements) {
        return joinToString(elements, "; ");
    }

    public static <T> String joinToString(Iterable<T> elements, CharSequence delimiter) {
        return joinToString(
                elements,
                delimiter,
                item -> item == null ? "" : item.toString());
    }

    public static <T> String joinToString(
            Iterable<T> elements,
            CharSequence delimiter,
            Function<? super T, String> elementMapper) {

        return StreamSupport.stream(elements.spliterator(), false)
                .map(elementMapper)
                .reduce((s, s2) -> s + delimiter + s2)
                .orElse(StringUtils.EMPTY);
    }
}
