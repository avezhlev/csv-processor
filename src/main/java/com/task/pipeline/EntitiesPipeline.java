package com.task.pipeline;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.stream.Stream;

@RequiredArgsConstructor
public class EntitiesPipeline<T> {

    @NonNull
    private final EntitiesProducer<T> producer;
    @NonNull
    private final EntitiesProcessor<T> processor;
    @NonNull
    private final EntitiesConsumer<T> consumer;

    public void execute() throws Exception {
        try (Stream<? extends T> input = producer.produce();
             Stream<? extends T> output = processor.process(input)) {
            consumer.consume(output);
        }
    }
}
