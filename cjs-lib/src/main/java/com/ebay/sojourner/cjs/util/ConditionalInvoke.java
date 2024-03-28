package com.ebay.sojourner.cjs.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ConditionalInvoke {

    public static void invokeIf(final boolean cond, final Runnable runnable) {
        invokeIf(cond, runnable, ConditionalInvoke::noop);
    }

    public static void invokeIf(final boolean cond, final Runnable runnable, final Runnable otherwise) {
        if (cond) runnable.run();
        else otherwise.run();
    }

    public static <T> void invokeIf(final T value, final Predicate<T> cond, final Consumer<T> consumer) {
        invokeIf(value, cond, consumer, ConditionalInvoke::noop);
    }

    public static <T> void invokeIf(final T value, final Predicate<T> cond,
                                    final Consumer<T> consumer, final Consumer<T> otherwise) {

        if (cond.test(value)) consumer.accept(value);
        else otherwise.accept(value);
    }

    public static <T> void invokeIfNotNull(final T value, final Consumer<T> consumer) {
        invokeIfNotNull(value, consumer, ConditionalInvoke::noop);
    }

    public static <T> void invokeIfNotNull(final T value, final Consumer<T> consumer, final Consumer<T> otherwise) {
        invokeIf(value, Objects::nonNull, consumer, otherwise);
    }

    public static <T> void invokeIfNotNull(final T value, final Predicate<T> alsoCond,
                                           final Consumer<T> consumer, final Consumer<T> otherwise) {

        invokeIf(value, v -> Objects.nonNull(v) && alsoCond.test(v), consumer, otherwise);
    }

    public static void invokeIfNotBlankStr(final String value, final Consumer<String> consumer) {
        invokeIfNotBlankStr(value, consumer, ConditionalInvoke::noop);
    }

    public static void invokeIfNotBlankStr(final String value,
                                           final Consumer<String> consumer, final Consumer<String> otherwise) {

        invokeIf(value, StringUtils::isNotBlank, consumer, otherwise);
    }

    public static <T> void noop(T t) {
    }

    public static void noop() {
    }

}
