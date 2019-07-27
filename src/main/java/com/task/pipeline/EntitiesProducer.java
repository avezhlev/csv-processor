package com.task.pipeline;

import java.util.stream.Stream;

public interface EntitiesProducer<T> {

    Stream<? extends T> produce() throws Exception;
}
