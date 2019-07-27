package com.task.pipeline.processor.util;

import lombok.NonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class EntitiesGrouper<T, ID, C extends Collection<T>> {

    private final Function<? super T, ? extends ID> idMapper;
    private final Supplier<C> groupSupplier;
    private final BiConsumer<C, T> groupAccumulator;
    private final BinaryOperator<C> groupsCombiner;

    private final Map<ID, C> groups;

    public EntitiesGrouper(@NonNull Function<? super T, ? extends ID> idMapper, @NonNull Supplier<C> groupSupplier,
                           @NonNull BiConsumer<C, T> groupAccumulator, BinaryOperator<C> groupsCombiner) {
        this.idMapper = idMapper;
        this.groupSupplier = groupSupplier;
        this.groupAccumulator = groupAccumulator;
        this.groupsCombiner = groupsCombiner;
        if (groupsCombiner == null) {
            // for concurrent usage where items are added by several threads and no groups merging is required
            groups = new ConcurrentHashMap<>();
        } else {
            // for fork-join usage where items are added by one thread and groups are then merged using a provided combiner
            groups = new HashMap<>();

        }
    }

    public EntitiesGrouper(Function<? super T, ? extends ID> idMapper, Supplier<C> groupSupplier, BiConsumer<C, T> groupAccumulator) {
        this(idMapper, groupSupplier, groupAccumulator, null);
    }

    public void add(T item) {
        groups.compute(idMapper.apply(item), (id, group) -> {
            if (group == null) {
                group = groupSupplier.get();
            }
            groupAccumulator.accept(group, item);
            return group;
        });
    }

    public EntitiesGrouper<T, ID, C> merge(EntitiesGrouper<T, ID, C> other) {
        other.groups.forEach((id, group) -> groups.merge(id, group, groupsCombiner));
        return this;
    }

    public Stream<? extends T> stream() {
        return groups.values().stream().flatMap(Collection::stream);
    }

}
