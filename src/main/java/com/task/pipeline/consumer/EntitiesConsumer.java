package com.task.pipeline.consumer;

import java.util.stream.Stream;

public interface EntitiesConsumer<ENTITY> {

    void consume(Stream<? extends ENTITY> entities) throws Exception;
}
