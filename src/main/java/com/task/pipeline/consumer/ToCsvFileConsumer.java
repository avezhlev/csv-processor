package com.task.pipeline.consumer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class ToCsvFileConsumer<ENTITY> implements EntitiesConsumer<ENTITY> {

    private static final CSVFormat DEFAULT_CSV_FORMAT = CSVFormat.DEFAULT;

    @NonNull
    private final Function<? super ENTITY, Object[]> fromEntityMapper;
    @NonNull
    private final Path file;
    @NonNull
    private final CSVFormat format;

    public static <ENTITY> ToCsvFileConsumer<ENTITY> withDefaultFormat(Function<? super ENTITY, Object[]> fromEntityMapper, Path file) {
        return new ToCsvFileConsumer<>(fromEntityMapper, file, DEFAULT_CSV_FORMAT);
    }

    @Override
    public void consume(Stream<? extends ENTITY> entities) throws IOException {
        try (Stream<Object[]> records = entities.map(fromEntityMapper);
             CSVPrinter printer = new CSVPrinter(Files.newBufferedWriter(file), format)) {
            for (Object[] record : (Iterable<Object[]>) records::iterator) {
                printer.printRecord(record);
            }
        }
    }
}
