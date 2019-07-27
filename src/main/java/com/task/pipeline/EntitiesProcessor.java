package com.task.pipeline;

import java.util.stream.Stream;

public interface EntitiesProcessor<T> {

    Stream<? extends T> process(Stream<? extends T> entities);
}
