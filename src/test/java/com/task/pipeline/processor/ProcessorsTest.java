package com.task.pipeline.processor;

import com.task.entity.SimpleEntity;
import com.task.pipeline.EntitiesProcessor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ProcessorsTest {

    public static final Comparator<SimpleEntity> DEFAULT_COMPARATOR =
            Comparator.comparingDouble(SimpleEntity::getPrice).thenComparingInt(SimpleEntity::getId);

    @ParameterizedTest
    @ValueSource(classes = {
            TimeOptimizedConcurrentGroupingProcessor.class,
            TimeOptimizedForkJoinGroupingProcessor.class,
            SpaceOptimizedProcessor.class})
    public void outputMustBeSortedAccordingToComparator(Class<? extends EntitiesProcessor> impl) {
        // given
        Comparator<SimpleEntity> comparator = DEFAULT_COMPARATOR;
        int totalLimit = 1000;
        int inputSize = totalLimit;
        EntitiesProcessor<SimpleEntity> processor = processor(impl, comparator, Integer.MAX_VALUE, totalLimit);
        Stream<SimpleEntity> input = withUniqueIdsAndRandomPrices(inputSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        Assertions.assertThat(output)
                .isSortedAccordingTo(comparator);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            TimeOptimizedConcurrentGroupingProcessor.class,
            TimeOptimizedForkJoinGroupingProcessor.class,
            SpaceOptimizedProcessor.class})
    public void outputMustBeLimitedIfInputExceedsTotalLimit(Class<? extends EntitiesProcessor> impl) {
        // given
        int totalLimit = 1000;
        int inputSize = totalLimit * 2;
        EntitiesProcessor<SimpleEntity> processor = processor(impl, Integer.MAX_VALUE, totalLimit);
        Stream<SimpleEntity> input = withUniqueIdsAndRandomPrices(inputSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        Assertions.assertThat(output)
                .hasSize(totalLimit);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            TimeOptimizedConcurrentGroupingProcessor.class,
            TimeOptimizedForkJoinGroupingProcessor.class,
            SpaceOptimizedProcessor.class})
    public void outputForGroupMustBeLimitedIfAnyInputGroupSizeExceedsNonZeroGroupLimit(Class<? extends EntitiesProcessor> impl) {
        // given
        int groupLimit = 20;
        int totalLimit = 1000;
        int groupSize = (int) (groupLimit * 1.5);
        int inputSize = totalLimit;
        EntitiesProcessor<SimpleEntity> processor = processor(impl, groupLimit, totalLimit);
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

    @ParameterizedTest
    @ValueSource(classes = {
            TimeOptimizedConcurrentGroupingProcessor.class,
            TimeOptimizedForkJoinGroupingProcessor.class,
            SpaceOptimizedProcessor.class})
    public void outputMustBeEmptyIfGroupLimitIsZero(Class<? extends EntitiesProcessor> impl) {
        // given
        int groupLimit = 0;
        int totalLimit = 10;
        int groupSize = 1;
        int inputSize = totalLimit;
        EntitiesProcessor<SimpleEntity> processor = processor(impl, groupLimit, totalLimit);
        Stream<SimpleEntity> input = withGroupedIdsAndRandomPrices(inputSize, groupSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        Assertions.assertThat(output)
                .hasSize(0);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            TimeOptimizedConcurrentGroupingProcessor.class,
            TimeOptimizedForkJoinGroupingProcessor.class,
            SpaceOptimizedProcessor.class})
    public void outputMustBeEmptyIfTotalLimitIsZero(Class<? extends EntitiesProcessor> impl) {
        // given
        int groupLimit = 1;
        int totalLimit = 0;
        int groupSize = 1;
        int inputSize = 10;
        EntitiesProcessor<SimpleEntity> processor = processor(impl, groupLimit, totalLimit);
        Stream<SimpleEntity> input = withGroupedIdsAndRandomPrices(inputSize, groupSize);
        // when
        Stream<? extends SimpleEntity> output = processor.process(input);
        // then
        Assertions.assertThat(output)
                .hasSize(0);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            TimeOptimizedConcurrentGroupingProcessor.class,
            TimeOptimizedForkJoinGroupingProcessor.class,
            SpaceOptimizedProcessor.class})
    public void outputMustBeSortedAccordingToComparatorAndLimitedIfInputExceedsTotalLimitAndAnyInputGroupSizeExceedsGroupLimit(Class<? extends EntitiesProcessor> impl) {
        // given
        Comparator<SimpleEntity> comparator = DEFAULT_COMPARATOR;
        int groupLimit = 20;
        int totalLimit = 1000;
        int groupSize = (int) (groupLimit * 1.5);
        int inputSize = totalLimit * 2;
        EntitiesProcessor<SimpleEntity> processor = processor(impl, comparator, groupLimit, totalLimit);
        Stream<SimpleEntity> input = withGroupedIdsAndRandomPrices(inputSize, groupSize);
        List<SimpleEntity> collectedInput = input.collect(Collectors.toList());
        Map<Integer, List<SimpleEntity>> inputSortedGroups = collectedInput.stream()
                .collect(Collectors.groupingBy(
                        SimpleEntity::getId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> list.stream().sorted(comparator).collect(Collectors.toList()))));
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
                .isSortedAccordingTo(comparator)
                .allMatch(entity -> {
                    long outputGroupSize = outputGroupSizes.get(entity.getId());
                    return outputGroupSize <= groupLimit
                            &&
                            (outputGroupSize == 0
                                    || comparator.compare(entity, inputSortedGroups.get(entity.getId()).get((int) outputGroupSize - 1)) <= 0);
                });
    }


    private EntitiesProcessor<SimpleEntity> processor(Class<? extends EntitiesProcessor> impl,
                                                      int maxEntitiesPerGroup, int maxTotalEntities) {
        return processor(impl, DEFAULT_COMPARATOR, maxEntitiesPerGroup, maxTotalEntities);
    }

    @SuppressWarnings("unchecked")
    private EntitiesProcessor<SimpleEntity> processor(Class<? extends EntitiesProcessor> impl,
                                                      Comparator<SimpleEntity> comparator,
                                                      int maxEntitiesPerGroup, int maxTotalEntities) {
        try {
            return impl.getDeclaredConstructor(Function.class, Comparator.class, int.class, int.class)
                    .newInstance(
                            (Function<SimpleEntity, Integer>) SimpleEntity::getId, comparator,
                            maxEntitiesPerGroup, maxTotalEntities);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Stream<SimpleEntity> withUniqueIdsAndRandomPrices(int count) {
        return IntStream.range(0, count).mapToObj(i -> new SimpleEntity(i, ThreadLocalRandom.current().nextDouble()));
    }

    private Stream<SimpleEntity> withGroupedIdsAndRandomPrices(int count, int groupSize) {
        return IntStream.range(0, count).mapToObj(i -> new SimpleEntity(i / groupSize, ThreadLocalRandom.current().nextDouble()));
    }
}
