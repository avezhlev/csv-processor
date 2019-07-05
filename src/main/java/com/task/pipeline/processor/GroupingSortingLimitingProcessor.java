package com.task.pipeline.processor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class GroupingSortingLimitingProcessor<ENTITY, GROUP> implements EntitiesProcessor<ENTITY> {

    private static final int DEFAULT_GROUP_LIMIT = 20;
    private static final long DEFAULT_TOTAL_LIMIT = 1000;

    @NonNull
    private final Function<? super ENTITY, ? extends GROUP> groupExtractor;
    @NonNull
    private final Comparator<ENTITY> entityComparator;
    private final int groupLimit;
    private final long totalLimit;

    public static <ENTITY, GROUP> GroupingSortingLimitingProcessor<ENTITY, GROUP>
    withDefaultLimits(Function<? super ENTITY, ? extends GROUP> groupExtractor, Comparator<ENTITY> entityComparator) {
        return new GroupingSortingLimitingProcessor<>(
                groupExtractor, entityComparator, DEFAULT_GROUP_LIMIT, DEFAULT_TOTAL_LIMIT);
    }

    @Override
    public Stream<? extends ENTITY> process(Stream<? extends ENTITY> entities) {
        return groupLimit == 0 || totalLimit == 0 ?
                Stream.empty() :
                entries(entities)
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
                    if (groupEntities.size() < groupLimit
                            || entityComparator.compare(entity, groupEntities.peek()) < 0) {
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
