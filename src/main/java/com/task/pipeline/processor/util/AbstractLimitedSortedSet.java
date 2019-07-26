package com.task.pipeline.processor.util;

import lombok.NonNull;

import java.util.*;

public abstract class AbstractLimitedSortedSet<T, SELF extends AbstractLimitedSortedSet<T, SELF>> extends TreeSet<T> {

    private final Comparator<? super T> comparator;
    private final int limit;

    public AbstractLimitedSortedSet(@NonNull Comparator<? super T> comparator, int limit) {
        super(comparator);
        this.comparator = comparator;
        this.limit = limit;
    }

    @SuppressWarnings("unchecked")
    public SELF merge(SELF other) {
        if (this.size() > other.size()) {
            addAll((other));
            return (SELF) this;
        } else {
            other.addAll(this);
            return other;
        }
    }

    @Override
    public boolean add(T item) {
        if (isCandidateForAdding(item)) {
            return doAdd(item);
        }
        return false;
    }

    protected abstract boolean doAdd(T item);

    /*package-private*/ boolean addToSet(T item) {
        return super.add(item);
    }

    /*package-private*/ boolean isLimitExceeded() {
        return size() > limit;
    }

    private boolean isCandidateForAdding(T item) {
        return size() < limit || comparator.compare(item, last()) < 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean addAll(Collection<? extends T> collection) {
        if (collection.size() == 0) {
            return false;
        }
        if (collection instanceof SortedSet
                && Objects.equals(this.comparator, ((SortedSet) collection).comparator())) {
            return doAddAll((SortedSet) collection);
        }
        return doAddAll(collection);
    }

    private boolean doAddAll(SortedSet<? extends T> sortedSet) {
        boolean changed = false;
        for (T item : sortedSet) {
            if (!isCandidateForAdding(item)) {
                break;
            }
            if (doAdd(item) && !changed) {
                changed = true;
            }
        }
        return changed;
    }

    private boolean doAddAll(Collection<? extends T> collection) {
        boolean changed = false;
        for (T item : collection) {
            if (add(item) && !changed) {
                changed = true;
            }
        }
        return changed;
    }

}
