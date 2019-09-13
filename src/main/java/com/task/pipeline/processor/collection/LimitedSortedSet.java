package com.task.pipeline.processor.collection;

import lombok.NonNull;

import java.util.Comparator;

public class LimitedSortedSet<T> extends AbstractLimitedSortedSet<T, LimitedSortedSet<T>> {

    public LimitedSortedSet(@NonNull Comparator<? super T> comparator, int limit) {
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
