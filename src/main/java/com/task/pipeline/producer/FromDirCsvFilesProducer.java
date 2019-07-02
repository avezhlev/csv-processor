package com.task.pipeline.producer;

import com.task.pipeline.EntitiesProducer;
import com.task.pipeline.util.BufferingStreamSplitter;
import com.task.pipeline.util.MappingResult;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor
public class FromDirCsvFilesProducer<T> implements EntitiesProducer<T> {

    private static final CSVFormat DEFAULT_CSV_FORMAT = CSVFormat.DEFAULT;

    @NonNull
    private final Function<CSVRecord, ? extends T> toEntityMapper;
    @NonNull
    private final Path dir;
    @NonNull
    private final CSVFormat format;

    public static <T> FromDirCsvFilesProducer<T> withDefaultFormat(Function<CSVRecord, ? extends T> toEntityMapper,
                                                                   Path dir) {
        return new FromDirCsvFilesProducer<>(toEntityMapper, dir, DEFAULT_CSV_FORMAT);
    }

    @Override
    public Stream<? extends T> produce() throws IOException {
        if (!Files.isDirectory(dir)) {
            throw new NotDirectoryException(dir.toAbsolutePath().toString());
        }
        return entities(records(files(dir)));
    }

    private Stream<? extends T> entities(Stream<CSVRecord> records) {
        return records
                .map(MappingResult.wrap(toEntityMapper::apply))
                .filter(MappingResult::isSuccessful) // TODO: process entity mapping failures?
                .map(MappingResult::getMappedValue);
    }

    private Stream<CSVRecord> records(Stream<? extends Path> files) {
        return files
                .map(MappingResult.wrap(Files::newBufferedReader))
                .filter(MappingResult::isSuccessful)
                .map(MappingResult::getMappedValue)
                .map(MappingResult.wrap(this::readerRecords))
                .peek(this::handleParserInitFailure)
                .filter(MappingResult::isSuccessful)
                .flatMap(MappingResult::getMappedValue);
    }

    private Stream<? extends Path> files(Path dir) throws IOException {
        return BufferingStreamSplitter.split(
                Files.list(dir)
                        .filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".csv")));
    }

    private Stream<CSVRecord> readerRecords(Reader reader) throws IOException {
        CSVParser parser = CSVParser.parse(reader, format);
        return StreamSupport
                .stream(parser.spliterator(), false)
                .onClose(() -> silentClose(parser));
    }

    private void handleParserInitFailure(MappingResult<? extends Reader, Stream<CSVRecord>> mappingResult) {
        if (!mappingResult.isSuccessful()) {
            silentClose(mappingResult.getInitialValue());
        }
    }

    private static void silentClose(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ignored) {
        }
    }

}
