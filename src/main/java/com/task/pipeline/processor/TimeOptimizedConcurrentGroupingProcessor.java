package com.task.pipeline.processor;

import com.task.pipeline.processor.util.EntitiesGrouper;
import com.task.pipeline.processor.util.SimpleLimitedSortedSet;
import lombok.NonNull;

import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * This implementation has O(N*(C1*logK + C2*logM)) time complexity and O(N) space complexity, where
 * - N is total input size
 * - M is total output limit
 * - K is output group size limit
 *
 * @param <T>  type of entities to process
 * @param <ID> type of entities groups identifier
 */
public class TimeOptimizedConcurrentGroupingProcessor<T, ID> extends AbstractGroupingLimitingSortingProcessor<T, ID> {

    public TimeOptimizedConcurrentGroupingProcessor(@NonNull Function<? super T, ? extends ID> idMapper,
                                                    @NonNull Comparator<? super T> comparator,
                                                    int groupLimit, int totalLimit) {
        super(idMapper, comparator, groupLimit, totalLimit);
    }

    public static <T, ID> TimeOptimizedConcurrentGroupingProcessor<T, ID> withDefaultLimits(Function<? super T, ? extends ID> idMapper,
                                                                                            Comparator<? super T> comparator) {
        return new TimeOptimizedConcurrentGroupingProcessor<>(idMapper, comparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    protected Stream<? extends T> groupLimitSort(Stream<? extends T> entities) {
        return limitSort(entities.parallel().collect(Collector.of(
                () -> new EntitiesGrouper<>(
                        getIdMapper(),
                        () -> new SimpleLimitedSortedSet<T>(getComparator(), getGroupLimit()),
                        SimpleLimitedSortedSet::add),
                EntitiesGrouper::add, EntitiesGrouper::merge, EntitiesGrouper::stream,
                Collector.Characteristics.CONCURRENT, Collector.Characteristics.UNORDERED)));
    }

}
