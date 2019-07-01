package com.task.pipeline.processor;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class GroupingWeightingLimitingProcessor<ENTITY, GROUP, WEIGHT> implements EntitiesProcessor<ENTITY> {

    private static final int DEFAULT_GROUP_LIMIT = 20;
    private static final long DEFAULT_TOTAL_LIMIT = 1000;

    @NonNull
    private final Function<? super ENTITY, ? extends GROUP> groupExtractor;
    @NonNull
    private final Function<? super ENTITY, ? extends WEIGHT> weightExtractor;
    @NonNull
    private final Comparator<WEIGHT> weightComparator;
    @NonNull
    private final Comparator<ENTITY> entityComparator;
    private final int groupLimit;
    private final long totalLimit;

    public GroupingWeightingLimitingProcessor(Function<? super ENTITY, ? extends GROUP> groupExtractor,
                                              Function<? super ENTITY, ? extends WEIGHT> weightExtractor,
                                              Comparator<WEIGHT> weightComparator,
                                              int groupLimit, long totalLimit) {
        this(groupExtractor, weightExtractor, weightComparator,
                Comparator.comparing(weightExtractor, weightComparator),
                groupLimit, totalLimit);
    }

    public static <ENTITY, GROUP, WEIGHT> GroupingWeightingLimitingProcessor<ENTITY, GROUP, WEIGHT>
    withDefaultLimits(Function<? super ENTITY, ? extends GROUP> groupExtractor,
                      Function<? super ENTITY, ? extends WEIGHT> weightExtractor,
                      Comparator<WEIGHT> weightComparator) {
        return new GroupingWeightingLimitingProcessor<>(
                groupExtractor, weightExtractor, weightComparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    public Stream<? extends ENTITY> process(Stream<? extends ENTITY> entities) {
        return entries(entities)
                .values()
                .parallelStream()
                .flatMap(Collection::stream)
                .sorted(entityComparator)
                .limit(totalLimit);
    }

    private Map<GROUP, ? extends Collection<? extends ENTITY>> entries(Stream<? extends ENTITY> entities) {
        ConcurrentMap<GROUP, Queue<ENTITY>> entries = new ConcurrentHashMap<>();
        entities.parallel().forEach(entity ->
                entries.compute(groupExtractor.apply(entity), (k, v) -> {
                    Queue<ENTITY> groupEntities =
                            v == null ?
                                    // have to reverse the comparator since PriorityQueue is min heap based
                                    new PriorityQueue<>(entityComparator.reversed()) :
                                    v;
                    Optional<WEIGHT> groupWeightLimit = Optional
                            .ofNullable(groupEntities.size() == groupLimit ? groupEntities.peek() : null)
                            .map(weightExtractor);
                    if (!groupWeightLimit.isPresent()
                            || weightComparator.compare(weightExtractor.apply(entity), groupWeightLimit.get()) < 0) {
                        groupEntities.add(entity);
                        if (groupEntities.size() > groupLimit) {
                            groupEntities.poll();
                        }
                    }
                    return groupEntities;
                }));
        return entries;
    }

}
