package com.task.pipeline;

import com.task.pipeline.consumer.EntitiesConsumer;
import com.task.pipeline.processor.EntitiesProcessor;
import com.task.pipeline.producer.EntitiesProducer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.stream.Stream;

@RequiredArgsConstructor
public class EntitiesPipeline<ENTITY> {

    @NonNull
    private final EntitiesProducer<ENTITY> entitiesProducer;
    @NonNull
    private final EntitiesProcessor<ENTITY> entitiesProcessor;
    @NonNull
    private final EntitiesConsumer<ENTITY> entitiesConsumer;

    public void execute() throws Exception {
        try (Stream<? extends ENTITY> inputPipeline = entitiesProducer.produce();
             Stream<? extends ENTITY> outputPipeline = entitiesProcessor.process(inputPipeline)) {
            entitiesConsumer.consume(outputPipeline);
        }
    }
}
