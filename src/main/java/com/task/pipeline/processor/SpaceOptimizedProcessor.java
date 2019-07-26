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
 * @param <T>  type of entities to process
 * @param <ID> type of entities groups identifier
 */
public class SpaceOptimizedProcessor<T, ID> extends AbstractGroupingLimitingSortingProcessor<T, ID> {

    public SpaceOptimizedProcessor(@NonNull Function<? super T, ? extends ID> idMapper,
                                   @NonNull Comparator<? super T> comparator,
                                   int groupLimit, int totalLimit) {
        super(idMapper, comparator, groupLimit, totalLimit);
    }

    public static <T, ID> SpaceOptimizedProcessor<T, ID> withDefaultLimits(Function<? super T, ? extends ID> idMapper,
                                                                           Comparator<? super T> comparator) {
        return new SpaceOptimizedProcessor<>(idMapper, comparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    protected Stream<? extends T> groupLimitSort(Stream<? extends T> entities) {
        return entities.parallel().collect(Collector.of(
                () -> new GroupingLimitedSortedSet<T, ID>(getIdMapper(), getComparator(), getGroupLimit(), getTotalLimit()),
                GroupingLimitedSortedSet::add, GroupingLimitedSortedSet::merge, GroupingLimitedSortedSet::stream,
                Collector.Characteristics.UNORDERED));
    }

}
