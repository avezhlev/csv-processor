package com.task.pipeline.processor.util;

import lombok.NonNull;

import java.util.Comparator;

public class SimpleLimitedSortedSet<T> extends AbstractLimitedSortedSet<T, SimpleLimitedSortedSet<T>> {

    public SimpleLimitedSortedSet(@NonNull Comparator<? super T> comparator, int limit) {
        super(comparator, limit);
    }

    @Override
    protected boolean doAdd(T item) {
        boolean added = addToSet(item);
        if (added && isLimitExceeded()) {
            pollLast();
        }
        return added;
    }
}
