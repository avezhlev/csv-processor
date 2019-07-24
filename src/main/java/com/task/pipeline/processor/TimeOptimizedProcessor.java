package com.task.pipeline.processor;

import com.task.pipeline.processor.util.SimpleLimitedSortedSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Grouping, sorting and limiting processor.
 * This implementation has O(N*(C1*logK + C2*logM)) time complexity and O(N) space complexity, where
 * - N is total input size
 * - M is total output limit
 * - K is output group size limit
 *
 * @param <ENTITY> type of entities to process
 * @param <GROUP>  type of entities groups identifier
 */
@RequiredArgsConstructor
public class TimeOptimizedProcessor<ENTITY, GROUP> implements EntitiesProcessor<ENTITY> {

    private static final int DEFAULT_GROUP_LIMIT = 20;
    private static final int DEFAULT_TOTAL_LIMIT = 1000;

    @NonNull
    private final Function<? super ENTITY, ? extends GROUP> groupExtractor;
    @NonNull
    private final Comparator<? super ENTITY> entityComparator;
    private final int groupLimit;
    private final int totalLimit;

    public static <ENTITY, GROUP> TimeOptimizedProcessor<ENTITY, GROUP>
    withDefaultLimits(Function<? super ENTITY, ? extends GROUP> groupExtractor, Comparator<ENTITY> entityComparator) {
        return new TimeOptimizedProcessor<>(
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
                        () -> new SimpleLimitedSortedSet<>(entityComparator, totalLimit),
                        SimpleLimitedSortedSet<ENTITY>::add,
                        SimpleLimitedSortedSet::merge))
                .stream();
    }

    private Map<GROUP, SimpleLimitedSortedSet<ENTITY>> groups(Stream<? extends ENTITY> entities) {
        Map<GROUP, SimpleLimitedSortedSet<ENTITY>> groups = new ConcurrentHashMap<>();
        entities.parallel().forEach(entity ->
                groups.compute(groupExtractor.apply(entity), (k, v) -> {
                    SimpleLimitedSortedSet<ENTITY> group = v == null ? new SimpleLimitedSortedSet<>(entityComparator, groupLimit) : v;
                    group.add(entity);
                    return group;
                }));
        return groups;
    }

}
