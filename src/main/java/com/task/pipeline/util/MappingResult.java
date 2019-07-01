package com.task.pipeline.util;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.function.Function;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class MappingResult<T, R> {

    private final T initialValue;
    private final Exception mappingException;
    private final R mappedValue;

    private static <T, R> MappingResult<T, R> success(R value) {
        return new MappingResult<>(null, null, value);
    }

    private static <T, R> MappingResult<T, R> failure(T initialValue, Exception exception) {
        return new MappingResult<>(initialValue, exception, null);
    }

    public static <T, R> Function<T, MappingResult<T, R>> wrap(CheckedFunction<T, R> function) {
        return t -> {
            try {
                return success(function.apply(t));
            } catch (Exception e) {
                return failure(t, e);
            }
        };
    }

    public boolean isSuccessful() {
        return mappedValue != null;
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        R apply(T t) throws Exception;
    }
}
