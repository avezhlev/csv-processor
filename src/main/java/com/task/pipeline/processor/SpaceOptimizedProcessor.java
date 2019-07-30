package com.task.pipeline.processor;

import com.task.pipeline.processor.util.collection.AbstractLimitedSortedSet;
import lombok.NonNull;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * This implementation has O(N*(M-K)*logM) time complexity but can reach O(M) space complexity, where
 * - N is total input size
 * - M is total output limit
 * - K is output group size limit
 *
 * @param <T>  type of entities to process
 * @param <ID> type of entities groups identifier
 */
public class SpaceOptimizedProcessor<T, ID> extends AbstractGroupingLimitingSortingProcessor<T, ID> {

    public SpaceOptimizedProcessor(@NonNull Function<? super T, ? extends ID> idMapper,
                                   @NonNull Comparator<? super T> comparator,
                                   int groupLimit, int totalLimit) {
        super(idMapper, comparator, groupLimit, totalLimit);
    }

    @Override
    protected Stream<? extends T> groupLimitSort(Stream<? extends T> entities) {
        return entities.parallel().collect(Collector.of(
                () -> new GroupingLimitedSortedSet<T, ID>(getIdMapper(), getComparator(), getGroupLimit(), getTotalLimit()),
                GroupingLimitedSortedSet::add, GroupingLimitedSortedSet::merge, GroupingLimitedSortedSet::stream,
                Collector.Characteristics.UNORDERED));
    }


    public static class GroupingLimitedSortedSet<T, ID> extends AbstractLimitedSortedSet<T, GroupingLimitedSortedSet<T, ID>> {

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

}
