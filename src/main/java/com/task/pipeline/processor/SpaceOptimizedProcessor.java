package com.task.pipeline.processor;

import com.task.pipeline.processor.util.GroupingLimitedSortedSet;
import lombok.NonNull;

import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * This implementation has O(N*(M-K)*logM) time complexity but can reach O(M) space complexity, where
 * - N is total input size
 * - M is total output limit
 * - K is output group size limit
 *
 * @param <ENTITY> type of entities to process
 * @param <GROUP>  type of entities groups identifier
 */
public class SpaceOptimizedProcessor<ENTITY, GROUP> extends AbstractGroupingLimitingSortingProcessor<ENTITY, GROUP> {

    public SpaceOptimizedProcessor(@NonNull Function<? super ENTITY, ? extends GROUP> groupExtractor,
                                   @NonNull Comparator<? super ENTITY> entityComparator,
                                   int groupLimit, int totalLimit) {
        super(groupExtractor, entityComparator, groupLimit, totalLimit);
    }

    public static <ENTITY, GROUP> SpaceOptimizedProcessor<ENTITY, GROUP> withDefaultLimits(Function<? super ENTITY, ? extends GROUP> groupExtractor,
                                                                                           Comparator<ENTITY> entityComparator) {
        return new SpaceOptimizedProcessor<>(groupExtractor, entityComparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    protected Stream<? extends ENTITY> groupLimitSort(Stream<? extends ENTITY> entities) {
        return entities.parallel().collect(Collector.of(
                () -> new GroupingLimitedSortedSet<ENTITY, GROUP>(getGroupExtractor(), getEntityComparator(), getGroupLimit(), getTotalLimit()),
                GroupingLimitedSortedSet::add, GroupingLimitedSortedSet::merge, GroupingLimitedSortedSet::stream,
                Collector.Characteristics.UNORDERED));
    }

}
