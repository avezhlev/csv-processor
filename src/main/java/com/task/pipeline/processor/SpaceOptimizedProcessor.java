package com.task.pipeline.processor;

import com.task.pipeline.processor.util.GroupingLimitedSortedSet;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Comparator;
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
                                    () -> new GroupingLimitedSortedSet<>(groupExtractor, entityComparator, groupLimit, totalLimit),
                                    GroupingLimitedSortedSet<ENTITY, GROUP>::add,
                                    GroupingLimitedSortedSet::merge))
                            .stream();
        } finally {
            entities.close();
        }
    }

}
