package com.task.pipeline.processor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class GroupingSortingLimitingProcessor<ENTITY, GROUP> implements EntitiesProcessor<ENTITY> {

    private static final int DEFAULT_GROUP_LIMIT = 20;
    private static final int DEFAULT_TOTAL_LIMIT = 1000;

    @NonNull
    private final Function<? super ENTITY, ? extends GROUP> groupExtractor;
    @NonNull
    private final Comparator<? super ENTITY> entityComparator;
    private final int groupLimit;
    private final int totalLimit;

    public static <ENTITY, GROUP> GroupingSortingLimitingProcessor<ENTITY, GROUP>
    withDefaultLimits(Function<? super ENTITY, ? extends GROUP> groupExtractor, Comparator<ENTITY> entityComparator) {
        return new GroupingSortingLimitingProcessor<>(
                groupExtractor, entityComparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    public Stream<? extends ENTITY> process(Stream<? extends ENTITY> entities) {
        try {
            return groupLimit == 0 || totalLimit == 0 ?
                    Stream.empty() :
                    flatMapLimitSort(groups(entities));
        } finally {
            entities.close();
        }
    }

    private Stream<ENTITY> flatMapLimitSort(Map<GROUP, ? extends Collection<ENTITY>> groups) {
        return groups.values()
                .parallelStream()
                .flatMap(Collection::stream)
                .collect(Collector.of(
                        () -> new LimitedSortedSet<>(entityComparator, totalLimit),
                        LimitedSortedSet<ENTITY>::add,
                        LimitedSortedSet::combine))
                .stream();
    }

    private Map<GROUP, LimitedSortedSet<ENTITY>> groups(Stream<? extends ENTITY> entities) {
        ConcurrentMap<GROUP, LimitedSortedSet<ENTITY>> groups = new ConcurrentHashMap<>();
        entities.parallel().forEach(entity ->
                groups.compute(groupExtractor.apply(entity), (k, v) -> {
                    LimitedSortedSet<ENTITY> group = v == null ? new LimitedSortedSet<>(entityComparator, groupLimit) : v;
                    group.add(entity);
                    return group;
                }));
        return groups;
    }


    private static class LimitedSortedSet<T> extends TreeSet<T> {

        private final Comparator<? super T> comparator;
        private final int limit;

        public LimitedSortedSet(@NonNull Comparator<? super T> comparator, int limit) {
            super(comparator);
            this.comparator = comparator;
            this.limit = limit;
        }

        public static <T> LimitedSortedSet<T> combine(LimitedSortedSet<T> first, LimitedSortedSet<T> second) {
            if (first.size() > second.size()) {
                first.addAll(second);
                return first;
            } else {
                second.addAll(first);
                return second;
            }
        }

        @Override
        public boolean add(T item) {
            if (isCandidateForAdding(item)) {
                return doAdd(item);
            }
            return false;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean addAll(Collection<? extends T> collection) {
            if (collection.size() == 0) {
                return false;
            }
            if (collection instanceof SortedSet
                    && Objects.equals(this.comparator, ((SortedSet) collection).comparator())) {
                return doAddAll((SortedSet<? extends T>) collection);
            }
            return doAddAll(collection);
        }

        private boolean isCandidateForAdding(T item) {
            return size() < limit || comparator.compare(item, last()) < 0;
        }

        private boolean doAdd(T item) {
            boolean added = super.add(item);
            if (added && size() > limit) {
                pollLast();
            }
            return added;
        }

        private boolean doAddAll(SortedSet<? extends T> set) {
            boolean changed = false;
            for (T item : set) {
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

}
