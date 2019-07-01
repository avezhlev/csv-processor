package com.task.pipeline.producer;

import java.util.stream.Stream;

public interface EntitiesProducer<ENTITY> {

    Stream<? extends ENTITY> produce() throws Exception;
}
