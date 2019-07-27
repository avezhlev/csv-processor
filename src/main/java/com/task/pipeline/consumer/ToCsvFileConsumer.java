package com.task.pipeline.consumer;

import com.task.pipeline.EntitiesConsumer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class ToCsvFileConsumer<T> implements EntitiesConsumer<T> {

    private static final CSVFormat DEFAULT_CSV_FORMAT = CSVFormat.DEFAULT;

    @NonNull
    private final Function<? super T, Object[]> fromEntityMapper;
    @NonNull
    private final Path file;
    @NonNull
    private final CSVFormat format;

    public static <T> ToCsvFileConsumer<T> withDefaultFormat(Function<? super T, Object[]> fromEntityMapper, Path file) {
        return new ToCsvFileConsumer<>(fromEntityMapper, file, DEFAULT_CSV_FORMAT);
    }

    @Override
    public void consume(Stream<? extends T> entities) throws IOException {
        try (Stream<Object[]> records = entities.map(fromEntityMapper);
             Writer writer = Files.newBufferedWriter(file);
             CSVPrinter printer = new CSVPrinter(writer, format)) {
            for (Object[] record : (Iterable<Object[]>) records::iterator) {
                printer.printRecord(record);
            }
        }
    }
}
