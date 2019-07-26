package com.task.pipeline.processor.util;

import lombok.NonNull;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class GroupingLimitedSortedSet<T, ID> extends AbstractLimitedSortedSet<T, GroupingLimitedSortedSet<T, ID>> {

    private final Function<? super T, ? extends ID> idMapper;
    private final int groupLimit;

    private final Map<ID, Integer> groupSizes = new HashMap<>();

    public GroupingLimitedSortedSet(@NonNull Function<? super T, ? extends ID> idMapper,
                                    @NonNull Comparator<? super T> comparator,
                                    int groupLimit, int totalLimit) {
        super(comparator, totalLimit);
        this.idMapper = idMapper;
        this.groupLimit = groupLimit;
    }

    @Override
    protected boolean doAdd(T item) {
        boolean added = addToSet(item);
        if (added) {
            ID id = idMapper.apply(item);
            int groupSize = groupSizes.getOrDefault(id, 0) + 1;
            if (groupSize > groupLimit) {
                Iterator<T> it = descendingIterator();
                T current;
                while (it.hasNext()) {
                    current = it.next();
                    if (id.equals(idMapper.apply(current))) {
                        it.remove();
                        if (current == item) {
                            added = false;
                        }
                        break;
                    }
                }
            } else {
                groupSizes.put(id, groupSize);
            }
            if (isLimitExceeded()) {
                T excluded = pollLast();
                groupSizes.compute(idMapper.apply(excluded), (k, size) ->
                        size != null && size > 1 ? size - 1 : null);
            }
        }
        return added;
    }
}
