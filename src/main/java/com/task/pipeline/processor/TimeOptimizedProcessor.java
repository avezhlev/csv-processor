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
 * This implementation has O(N*(C1*logK + C2*logM)) time complexity and O(N) space complexity, where
 * - N is total input size
 * - M is total output limit
 * - K is output group size limit
 *
 * @param <ENTITY> type of entities to process
 * @param <GROUP>  type of entities groups identifier
 */
public class TimeOptimizedProcessor<ENTITY, GROUP> extends AbstractGroupingLimitingSortingProcessor<ENTITY, GROUP> {

    public TimeOptimizedProcessor(@NonNull Function<? super ENTITY, ? extends GROUP> groupExtractor,
                                  @NonNull Comparator<? super ENTITY> entityComparator,
                                  int groupLimit, int totalLimit) {
        super(groupExtractor, entityComparator, groupLimit, totalLimit);
    }

    public static <ENTITY, GROUP> TimeOptimizedProcessor<ENTITY, GROUP> withDefaultLimits(Function<? super ENTITY, ? extends GROUP> groupExtractor,
                                                                                          Comparator<ENTITY> entityComparator) {
        return new TimeOptimizedProcessor<>(groupExtractor, entityComparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    protected Stream<? extends ENTITY> groupLimitSort(Stream<? extends ENTITY> entities) {
        return limitSort(groups(entities).values().stream().flatMap(Collection::stream));
    }

    private Map<GROUP, SimpleLimitedSortedSet<ENTITY>> groups(Stream<? extends ENTITY> entities) {
        Map<GROUP, SimpleLimitedSortedSet<ENTITY>> groups = new ConcurrentHashMap<>();
        entities.parallel().forEach(entity ->
                groups.compute(getGroupExtractor().apply(entity), (k, v) -> {
                    SimpleLimitedSortedSet<ENTITY> group =
                            v == null ? new SimpleLimitedSortedSet<>(getEntityComparator(), getGroupLimit()) : v;
                    group.add(entity);
                    return group;
                }));
        return groups;
    }

}
