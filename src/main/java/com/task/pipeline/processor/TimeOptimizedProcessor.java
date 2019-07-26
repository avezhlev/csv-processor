package com.task.pipeline.processor;

import com.task.pipeline.processor.util.SimpleLimitedSortedSet;
import lombok.NonNull;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
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
public class TimeOptimizedProcessor<T, ID> extends AbstractGroupingLimitingSortingProcessor<T, ID> {

    public TimeOptimizedProcessor(@NonNull Function<? super T, ? extends ID> idMapper,
                                  @NonNull Comparator<? super T> comparator,
                                  int groupLimit, int totalLimit) {
        super(idMapper, comparator, groupLimit, totalLimit);
    }

    public static <T, ID> TimeOptimizedProcessor<T, ID> withDefaultLimits(Function<? super T, ? extends ID> idMapper,
                                                                          Comparator<? super T> comparator) {
        return new TimeOptimizedProcessor<>(idMapper, comparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    protected Stream<? extends T> groupLimitSort(Stream<? extends T> entities) {
        return limitSort(groups(entities).values().stream().flatMap(Collection::stream));
    }

    private Map<ID, SimpleLimitedSortedSet<T>> groups(Stream<? extends T> entities) {
        Map<ID, SimpleLimitedSortedSet<T>> groups = new ConcurrentHashMap<>();
        entities.parallel().forEach(entity ->
                groups.compute(getIdMapper().apply(entity), (id, group) -> {
                    if (group == null) {
                        group = new SimpleLimitedSortedSet<>(getComparator(), getGroupLimit());
                    }
                    group.add(entity);
                    return group;
                }));
        return groups;
    }

}
