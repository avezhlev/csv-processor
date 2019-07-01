package com.task.pipeline.processor;

import java.util.stream.Stream;

public interface EntitiesProcessor<ENTITY> {

    Stream<? extends ENTITY> process(Stream<? extends ENTITY> entities);
}
