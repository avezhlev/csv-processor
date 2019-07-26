package com.task.pipeline.processor;

import com.task.pipeline.processor.util.SimpleLimitedSortedSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

@Getter
@RequiredArgsConstructor
public abstract class AbstractGroupingLimitingSortingProcessor<ENTITY, GROUP> implements EntitiesProcessor<ENTITY> {

    public static final int DEFAULT_GROUP_LIMIT = 20;
    public static final int DEFAULT_TOTAL_LIMIT = 1000;

    @NonNull
    private final Function<? super ENTITY, ? extends GROUP> groupExtractor;
    @NonNull
    private final Comparator<? super ENTITY> entityComparator;
    private final int groupLimit;
    private final int totalLimit;

    @Override
    public Stream<? extends ENTITY> process(Stream<? extends ENTITY> entities) {
        try {
            return groupLimit <= 0 || totalLimit <= 0 ?
                    Stream.empty() :
                    groupLimit < totalLimit ? groupLimitSort(entities) : limitSort(entities);
        } finally {
            entities.close();
        }
    }

    protected abstract Stream<? extends ENTITY> groupLimitSort(Stream<? extends ENTITY> entities);

    /*package-private*/ Stream<? extends ENTITY> limitSort(Stream<? extends ENTITY> entities) {
        return entities.parallel().collect(Collector.of(
                () -> new SimpleLimitedSortedSet<ENTITY>(entityComparator, totalLimit),
                SimpleLimitedSortedSet::add, SimpleLimitedSortedSet::merge, SimpleLimitedSortedSet::stream,
                Collector.Characteristics.UNORDERED));
    }

}
