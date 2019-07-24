package com.task.pipeline.processor.util;

import lombok.NonNull;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class GroupingLimitedSortedSet<ENTITY, GROUP> extends AbstractLimitedSortedSet<ENTITY, GroupingLimitedSortedSet<ENTITY, GROUP>> {

    private final Function<? super ENTITY, ? extends GROUP> groupExtractor;
    private final int groupLimit;

    private final Map<GROUP, Integer> groupSizes = new HashMap<>();

    public GroupingLimitedSortedSet(@NonNull Function<? super ENTITY, ? extends GROUP> groupExtractor,
                                    @NonNull Comparator<? super ENTITY> entityComparator,
                                    int groupLimit, int totalLimit) {
        super(entityComparator, totalLimit);
        this.groupExtractor = groupExtractor;
        this.groupLimit = groupLimit;
    }

    @Override
    protected boolean doAdd(ENTITY entity) {
        boolean added = addToSet(entity);
        if (added) {
            GROUP group = groupExtractor.apply(entity);
            int groupSize = groupSizes.getOrDefault(group, 0) + 1;
            if (groupSize > groupLimit) {
                Iterator<ENTITY> it = descendingIterator();
                ENTITY current;
                while (it.hasNext()) {
                    current = it.next();
                    if (group.equals(groupExtractor.apply(current))) {
                        it.remove();
                        if (current == entity) {
                            added = false;
                        }
                        break;
                    }
                }
            } else {
                groupSizes.put(group, groupSize);
            }
            if (isLimitExceeded()) {
                ENTITY excluded = pollLast();
                groupSizes.compute(groupExtractor.apply(excluded), (k, size) ->
                        size != null && size > 1 ? size - 1 : null);
            }
        }
        return added;
    }
}
