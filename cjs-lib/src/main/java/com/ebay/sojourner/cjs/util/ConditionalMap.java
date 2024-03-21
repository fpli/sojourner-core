package com.ebay.sojourner.cjs.util;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ConditionalMap {

    public static <T> T mapIf(final boolean cond, final Supplier<T> supplier) {
        return mapIf(cond, supplier, () -> null);
    }

    public static <T> T mapIf(final boolean cond, final Supplier<T> supplier, final Supplier<T> otherwise) {
        if (cond) return supplier.get();
        else return otherwise.get();
    }

    public static <S, T> T mapIf(final S value, final Predicate<S> cond, final Function<S, T> function) {
        return mapIf(value, cond, function, v -> null);
    }

    public static <S, T> T mapIf(final S value, final Predicate<S> cond,
                                 final Function<S, T> function, final Function<S, T> otherwise) {
        if (cond.test(value)) return function.apply(value);
        else return otherwise.apply(value);
    }

    public static <S, T> T mapIfNotNull(final S value, final Function<S, T> function) {
        return mapIfNotNull(value, function, v -> null);
    }

    public static <S, T> T mapIfNotNull(final S value, final Function<S, T> function, final Function<S, T> otherwise) {
        return mapIf(value, Objects::nonNull, function, otherwise);
    }

    public static <S, T> T mapIfNotNull(final S value, final Predicate<S> alsoCond,
                                        final Function<S, T> function) {
        return mapIfNotNull(value, alsoCond, function, v -> null);
    }

    public static <S, T> T mapIfNotNull(final S value, final Predicate<S> alsoCond,
                                        final Function<S, T> function, final Function<S, T> otherwise) {
        return mapIf(value, x -> Objects.nonNull(x) && alsoCond.test(x), function, otherwise);
    }

}
