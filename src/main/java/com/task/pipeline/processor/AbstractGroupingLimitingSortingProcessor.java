package com.task.pipeline.processor;

import com.task.pipeline.EntitiesProcessor;
import com.task.pipeline.processor.util.collection.LimitedSortedSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

@Getter
@RequiredArgsConstructor
public abstract class AbstractGroupingLimitingSortingProcessor<T, ID> implements EntitiesProcessor<T> {

    public static final int DEFAULT_GROUP_LIMIT = 20;
    public static final int DEFAULT_TOTAL_LIMIT = 1000;

    @NonNull
    private final Function<? super T, ? extends ID> idMapper;
    @NonNull
    private final Comparator<? super T> comparator;
    private final int groupLimit;
    private final int totalLimit;

    @Override
    public Stream<? extends T> process(Stream<? extends T> entities) {
        try {
            return groupLimit <= 0 || totalLimit <= 0 ?
                    Stream.empty() :
                    groupLimit < totalLimit ? groupLimitSort(entities) : limitSort(entities);
        } finally {
            entities.close();
        }
    }

    protected abstract Stream<? extends T> groupLimitSort(Stream<? extends T> entities);

    /*package-private*/ Stream<? extends T> limitSort(Stream<? extends T> entities) {
        return entities.parallel().collect(Collector.of(
                () -> new LimitedSortedSet<T>(comparator, totalLimit),
                LimitedSortedSet::add, LimitedSortedSet::merge, LimitedSortedSet::stream,
                Collector.Characteristics.UNORDERED));
    }

}
