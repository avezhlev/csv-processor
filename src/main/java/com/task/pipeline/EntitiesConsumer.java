package com.task.pipeline;

import java.util.stream.Stream;

public interface EntitiesConsumer<T> {

    void consume(Stream<? extends T> entities) throws Exception;
}
