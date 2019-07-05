package com.task.pipeline.processor;

import com.task.entity.SimpleEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class GroupingSortingLimitingProcessorTest {

    private static final Random random = new Random();

    @Test
    public void outputMustBeSortedAccordingToWeightComparator() {
        // given
        Comparator<SimpleEntity> entityComparator = Comparator.comparing(SimpleEntity::getPrice);
        int totalLimit = 1000;
        int inputSize = totalLimit;
        GroupingSortingLimitingProcessor<SimpleEntity, Integer> processor = processor(entityComparator, Integer.MAX_VALUE, totalLimit);
        Stream<SimpleEntity> input = withUniqueIdsAndRandomPrices(inputSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        Assertions.assertThat(output)
                .isSortedAccordingTo(entityComparator);
    }

    @Test
    public void outputMustBeLimitedIfInputExceedsTotalLimit() {
        // given
        int totalLimit = 1000;
        int inputSize = totalLimit * 2;
        GroupingSortingLimitingProcessor<SimpleEntity, Integer> processor = processor(Integer.MAX_VALUE, totalLimit);
        Stream<SimpleEntity> input = withUniqueIdsAndRandomPrices(inputSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        Assertions.assertThat(output)
                .hasSize(totalLimit);
    }

    @Test
    public void outputForGroupMustBeLimitedIfAnyInputGroupSizeExceedsNonZeroGroupLimit() {
        // given
        int groupLimit = 20;
        int totalLimit = 1000;
        int groupSize = (int) (groupLimit * 1.5);
        int inputSize = totalLimit;
        GroupingSortingLimitingProcessor<SimpleEntity, Integer> processor = processor(groupLimit, totalLimit);
        Stream<SimpleEntity> input = withGroupedIdsAndRandomPrices(inputSize, groupSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        int expectedSize = inputSize - inputSize / groupSize * (groupSize - groupLimit);
        List<SimpleEntity> collectedOutput = output.collect(Collectors.toList());
        Map<Integer, Long> outputGroupSizes = collectedOutput.stream()
                .collect(Collectors.groupingBy(
                        SimpleEntity::getId,
                        Collectors.counting()));
        Assertions.assertThat(collectedOutput)
                .hasSize(expectedSize);
        Assertions.assertThat(outputGroupSizes.values())
                .allMatch(outputGroupSize -> outputGroupSize <= groupLimit);
    }

    @Test
    public void outputMustBeEmptyIfGroupLimitIsZero() {
        // given
        int groupLimit = 0;
        int totalLimit = 10;
        int groupSize = 1;
        int inputSize = totalLimit;
        GroupingSortingLimitingProcessor<SimpleEntity, Integer> processor = processor(groupLimit, totalLimit);
        Stream<SimpleEntity> input = withGroupedIdsAndRandomPrices(inputSize, groupSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        Assertions.assertThat(output)
                .hasSize(0);
    }

    @Test
    public void outputMustBeEmptyIfTotalLimitIsZero() {
        // given
        int groupLimit = 1;
        int totalLimit = 0;
        int groupSize = 1;
        int inputSize = 10;
        GroupingSortingLimitingProcessor<SimpleEntity, Integer> processor = processor(groupLimit, totalLimit);
        Stream<SimpleEntity> input = withGroupedIdsAndRandomPrices(inputSize, groupSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        Assertions.assertThat(output)
                .hasSize(0);
    }

    @Test
    public void outputMustBeSortedAccordingToWeightComparatorAndLimitedIfInputExceedsTotalLimitAndAnyInputGroupSizeExceedsGroupLimit() {
        // given
        Comparator<SimpleEntity> entityComparator = Comparator.comparing(SimpleEntity::getPrice);
        int groupLimit = 20;
        int totalLimit = 1000;
        int groupSize = (int) (groupLimit * 1.5);
        int inputSize = totalLimit * 2;
        GroupingSortingLimitingProcessor<SimpleEntity, Integer> processor = processor(entityComparator, groupLimit, totalLimit);
        Stream<SimpleEntity> input = withGroupedIdsAndRandomPrices(inputSize, groupSize);
        List<SimpleEntity> collectedInput = input.collect(Collectors.toList());
        Map<Integer, List<SimpleEntity>> inputSortedGroups = collectedInput.stream()
                .collect(Collectors.groupingBy(
                        SimpleEntity::getId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> list.stream().sorted(entityComparator).collect(Collectors.toList()))));
        // when
        Stream<? extends SimpleEntity> output = processor.process(collectedInput.stream());
        // then
        int expectedSize = Math.min(totalLimit, inputSize - inputSize / groupSize * (groupSize - groupLimit));
        List<SimpleEntity> collectedOutput = output.collect(Collectors.toList());
        Map<Integer, Long> outputGroupSizes = collectedOutput.stream()
                .collect(Collectors.groupingBy(
                        SimpleEntity::getId,
                        Collectors.counting()));
        Assertions.assertThat(collectedOutput)
                .hasSize(expectedSize)
                .isSortedAccordingTo(entityComparator)
                .allMatch(entity -> {
                    long outputGroupSize = outputGroupSizes.get(entity.getId());
                    return outputGroupSize <= groupLimit
                            &&
                            (outputGroupSize == 0
                                    || entityComparator.compare(entity, inputSortedGroups.get(entity.getId()).get((int) outputGroupSize - 1)) <= 0);
                });
    }

    private GroupingSortingLimitingProcessor<SimpleEntity, Integer> processor(int maxEntitiesPerGroup, long maxTotalEntities) {
        return new GroupingSortingLimitingProcessor<>(SimpleEntity::getId, Comparator.comparing(SimpleEntity::getPrice),
                maxEntitiesPerGroup, maxTotalEntities);
    }

    private GroupingSortingLimitingProcessor<SimpleEntity, Integer> processor(Comparator<SimpleEntity> entityComparator,
                                                                              int maxEntitiesPerGroup, long maxTotalEntities) {
        return new GroupingSortingLimitingProcessor<>(SimpleEntity::getId, entityComparator, maxEntitiesPerGroup, maxTotalEntities);
    }

    private Stream<SimpleEntity> withUniqueIdsAndRandomPrices(int count) {
        return IntStream.range(0, count).mapToObj(i -> new SimpleEntity(i, random.nextDouble()));
    }

    private Stream<SimpleEntity> withGroupedIdsAndRandomPrices(int count, int groupSize) {
        return IntStream.range(0, count).mapToObj(i -> new SimpleEntity(i / groupSize, random.nextDouble()));
    }
}
