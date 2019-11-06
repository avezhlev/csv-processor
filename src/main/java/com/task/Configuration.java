package com.task;

import com.task.pipeline.EntitiesProcessor;
import com.task.pipeline.processor.SpaceOptimizedProcessor;
import com.task.pipeline.processor.TimeOptimizedConcurrentGroupingProcessor;
import com.task.pipeline.processor.TimeOptimizedForkJoinGroupingProcessor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.function.Function;

@Getter
@CommandLine.Command(name = "csv-processor", sortOptions = false, showDefaultValues = true)
public class Configuration {

    @CommandLine.Option(names = {"-i", "--input"}, required = true, description = "Input directory path")
    private Path inputDir;
    @CommandLine.Option(names = {"-o", "--output"}, required = true, description = "Output file path")
    private Path outputFile;
    @CommandLine.Option(names = {"-g", "--group"}, defaultValue = "20", description = "Group by ID limit")
    private int groupLimit;
    @CommandLine.Option(names = {"-l", "--limit"}, defaultValue = "1000", description = "Total output limit")
    private int limit;
    @CommandLine.Option(names = {"-p", "--processor"}, defaultValue = "SO",
            description = "Processor implementation\n" +
                    "Valid values: ${COMPLETION-CANDIDATES}\n" +
                    " TOCG: Time-optimized with concurrent grouping\n" +
                    " TOFJG: Time-optimized with fork-join grouping\n" +
                    " SO: Space-optimized")
    private Processor processor;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message")
    private boolean usageHelpRequested;

    public static Configuration parse(String[] args) {
        Configuration configuration = new Configuration();
        CommandLine commandLine = new CommandLine(configuration).setCaseInsensitiveEnumValuesAllowed(true);
        try {
            commandLine.parseArgs(args);
            if (commandLine.isUsageHelpRequested()) {
                commandLine.usage(System.out);
                System.exit(0);
            }
        } catch (CommandLine.ParameterException e) {
            System.out.println("Configuration parsing error: " + e.getMessage());
            commandLine.usage(System.out);
            System.exit(1);
        }
        return configuration;
    }


    @Getter
    @RequiredArgsConstructor
    public enum Processor {

        TOCG(TimeOptimizedConcurrentGroupingProcessor.class),
        TOFJG(TimeOptimizedForkJoinGroupingProcessor.class),
        SO(SpaceOptimizedProcessor.class);

        private final Class<? extends EntitiesProcessor> impl;

        @SuppressWarnings("unchecked")
        public <T> EntitiesProcessor<T> instantiate(@NonNull Function<? super T, ?> idMapper,
                                                    @NonNull Comparator<? super T> comparator,
                                                    int groupLimit, int limit) {
            try {
                return impl.getDeclaredConstructor(Function.class, Comparator.class, int.class, int.class)
                        .newInstance(idMapper, comparator, groupLimit, limit);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
