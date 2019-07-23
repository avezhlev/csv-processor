package com.task.pipeline.processor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Grouping, sorting and limiting processor.
 * This implementation has O(N*(M-K)*logM) time complexity but can reach O(M) space complexity, where
 * - N is total input size
 * - M is total output limit
 * - K is output group size limit
 *
 * @param <ENTITY> type of entities to process
 * @param <GROUP>  type of entities groups identifier
 */
@RequiredArgsConstructor
public class SpaceOptimizedProcessor<ENTITY, GROUP> implements EntitiesProcessor<ENTITY> {

    private static final int DEFAULT_GROUP_LIMIT = 20;
    private static final int DEFAULT_TOTAL_LIMIT = 1000;

    @NonNull
    private final Function<? super ENTITY, ? extends GROUP> groupExtractor;
    @NonNull
    private final Comparator<? super ENTITY> entityComparator;
    private final int groupLimit;
    private final int totalLimit;

    public static <ENTITY, GROUP> SpaceOptimizedProcessor<ENTITY, GROUP>
    withDefaultLimits(Function<? super ENTITY, ? extends GROUP> groupExtractor, Comparator<ENTITY> entityComparator) {
        return new SpaceOptimizedProcessor<>(
                groupExtractor, entityComparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    public Stream<? extends ENTITY> process(Stream<? extends ENTITY> entities) {
        try {
            return groupLimit == 0 || totalLimit == 0 ?
                    Stream.empty() :
                    entities.parallel()
                            .collect(Collector.of(
                                    () -> new GroupedLimitedSortedSet<>(groupExtractor, entityComparator, groupLimit, totalLimit),
                                    GroupedLimitedSortedSet<ENTITY, GROUP>::add,
                                    GroupedLimitedSortedSet::combine))
                            .stream();
        } finally {
            entities.close();
        }
    }


    private static class GroupedLimitedSortedSet<ENTITY, GROUP> extends TreeSet<ENTITY> {

        private final Function<? super ENTITY, ? extends GROUP> groupExtractor;
        private final Comparator<? super ENTITY> entityComparator;
        private final int groupLimit;
        private final int totalLimit;

        private final Map<GROUP, Integer> groupSizes = new HashMap<>();

        public GroupedLimitedSortedSet(@NonNull Function<? super ENTITY, ? extends GROUP> groupExtractor,
                                       @NonNull Comparator<? super ENTITY> entityComparator,
                                       int groupLimit, int totalLimit) {
            super(entityComparator);
            this.groupExtractor = groupExtractor;
            this.entityComparator = entityComparator;
            this.groupLimit = groupLimit;
            this.totalLimit = totalLimit;
        }

        public static <ENTITY, GROUP> GroupedLimitedSortedSet<ENTITY, GROUP> combine(GroupedLimitedSortedSet<ENTITY, GROUP> first,
                                                                                     GroupedLimitedSortedSet<ENTITY, GROUP> second) {
            if (first.size() > second.size()) {
                first.addAll(second);
                return first;
            } else {
                second.addAll(first);
                return second;
            }
        }

        @Override
        public boolean add(ENTITY entity) {
            if (isCandidateForAdding(entity)) {
                return doAdd(entity);
            }
            return false;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean addAll(Collection<? extends ENTITY> collection) {
            if (collection.size() == 0) {
                return false;
            }
            if (collection instanceof SortedSet
                    && Objects.equals(this.entityComparator, ((SortedSet) collection).comparator())) {
                return doAddAll((SortedSet<? extends ENTITY>) collection);
            }
            return doAddAll(collection);
        }

        private boolean isCandidateForAdding(ENTITY entity) {
            return size() < totalLimit || entityComparator.compare(entity, last()) < 0;
        }

        @SuppressWarnings("ConstantConditions")
        private boolean doAdd(ENTITY entity) {
            boolean added = super.add(entity);
            if (added) {
                GROUP group = groupExtractor.apply(entity);
                int groupSize = groupSizes.getOrDefault(group, 0) + 1;
                if (groupSize > groupLimit) {
                    Iterator<ENTITY> it = descendingIterator();
                    while (it.hasNext()) {
                        ENTITY current = it.next();
                        if (group.equals(groupExtractor.apply(current))) {
                            it.remove();
                            if (current == entity) {
                                added = false;
                            }
                            break;
                        }
                    }
                } else {
                    groupSizes.put(group, groupSize);
                }
                if (size() > totalLimit) {
                    groupSizes.compute(groupExtractor.apply(pollLast()), (k, size) -> size > 1 ? size - 1 : null);
                }
            }
            return added;
        }

        private boolean doAddAll(SortedSet<? extends ENTITY> set) {
            boolean changed = false;
            for (ENTITY entity : set) {
                if (!isCandidateForAdding(entity)) {
                    break;
                }
                if (doAdd(entity) && !changed) {
                    changed = true;
                }
            }
            return changed;
        }

        private boolean doAddAll(Collection<? extends ENTITY> collection) {
            boolean changed = false;
            for (ENTITY entity : collection) {
                if (add(entity) && !changed) {
                    changed = true;
                }
            }
            return changed;
        }

    }


}
