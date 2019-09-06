package com.task.pipeline.processor;

import com.task.pipeline.processor.util.collection.LimitedSortedSet;
import lombok.NonNull;

import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
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
public class TimeOptimizedForkJoinGroupingProcessor<T, ID> extends AbstractGroupingLimitingSortingProcessor<T, ID> {

    public TimeOptimizedForkJoinGroupingProcessor(@NonNull Function<? super T, ? extends ID> idMapper,
                                                  @NonNull Comparator<? super T> comparator,
                                                  int groupLimit, int totalLimit) {
        super(idMapper, comparator, groupLimit, totalLimit);
    }

    @Override
    protected Stream<? extends T> groupLimitSort(Stream<? extends T> entities) {
        return limitSort(entities.parallel().collect(
                Collectors.groupingBy(getIdMapper(), Collector.of(
                        () -> new LimitedSortedSet<T>(getComparator(), getGroupLimit()),
                        LimitedSortedSet::add, LimitedSortedSet::merge, LimitedSortedSet::stream,
                        Collector.Characteristics.UNORDERED)))
                .values()
                .stream()
                .flatMap(Function.identity()));
    }

}
