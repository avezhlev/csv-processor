package com.task;

import com.task.entity.Product;
import com.task.pipeline.EntitiesPipeline;
import com.task.pipeline.EntitiesProcessor;
import com.task.pipeline.consumer.ToCsvFileConsumer;
import com.task.pipeline.processor.SpaceOptimizedProcessor;
import com.task.pipeline.processor.TimeOptimizedConcurrentGroupingProcessor;
import com.task.pipeline.processor.TimeOptimizedForkJoinGroupingProcessor;
import com.task.pipeline.producer.FromDirCsvFilesProducer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.function.Function;

public class Runner {

    public static void main(String[] args) {
        Configuration config = null;
        try {
            config = Configuration.parse(args);
        } catch (RuntimeException e) {
            System.out.println("Config parsing error: " + e.toString());
            printUsage();
            System.exit(1);
        }
        EntitiesPipeline<Product> pipeline = configurePipeline(config);
        System.out.println("Processing...");
        System.out.println(config.toString());
        try {
            pipeline.execute();
            System.out.println("Processed successfully");
        } catch (Exception e) {
            System.out.println("Processing error: " + e);
            System.exit(1);
        }
    }

    private static EntitiesPipeline<Product> configurePipeline(Configuration config) {
        return new EntitiesPipeline<>(
                FromDirCsvFilesProducer.withDefaultFormat(Product::parse, config.getInputDir()),
                config.getProcessor().instantiate(
                        Product::getId,
                        Comparator.comparingDouble(Product::getPrice)
                                .thenComparingInt(Product::getId)
                                .thenComparing(Product::getCondition)
                                .thenComparing(Product::getState),
                        config.getGroupLimit(), config.getLimit()),
                ToCsvFileConsumer.withDefaultFormat(Product::asFieldsArray, config.getOutputFile()));
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar <jar_name> <arg_name> <arg_value>\n" +
                "Args:\n" +
                " -i Input directory path, required\n" +
                " -o Output file path, required\n" +
                " -p Processor implementation, optional, default is SO\n" +
                "    * TOCG Time-optimized with concurrent grouping\n" +
                "    * TOFJG Time-optimized with fork-join grouping\n" +
                "    * SO Space-optimized\n" +
                " -g Group by ID limit, optional, default is 20\n" +
                " -l Total output limit, optional, default is 1000");
    }


    @Getter
    @Builder
    private static class Configuration {

        private static Processor DEFAULT_PROCESSOR = Processor.SO;
        private static int DEFAULT_GROUP_LIMIT = 20;
        private static int DEFAULT_LIMIT = 1000;

        @NonNull
        private final Path inputDir;
        @NonNull
        private final Path outputFile;
        @NonNull
        @Builder.Default
        private final Processor processor = DEFAULT_PROCESSOR;
        @Builder.Default
        private final int groupLimit = DEFAULT_GROUP_LIMIT;
        @Builder.Default
        private final int limit = DEFAULT_LIMIT;

        public static Configuration parse(String[] args) {
            ConfigurationBuilder builder = builder();
            for (int i = 0; i < args.length; i++) {
                // TODO: process runtime exceptions to more specifically inform user about config issues
                switch (args[i]) {
                    case "-i":
                        builder.inputDir(Paths.get(args[++i]));
                        break;
                    case "-o":
                        builder.outputFile(Paths.get(args[++i]));
                        break;
                    case "-p":
                        builder.processor(Processor.valueOf(args[++i].toUpperCase()));
                        break;
                    case "-g":
                        builder.groupLimit(Integer.parseInt(args[++i]));
                        break;
                    case "-l":
                        builder.limit(Integer.parseInt(args[++i]));
                        break;
                }
            }
            return builder.build();
        }

        @Override
        public String toString() {
            return "* inputDir=" + inputDir.toAbsolutePath() +
                    "\n* outputFile=" + outputFile.toAbsolutePath() +
                    "\n* processor=" + processor.getImpl().getSimpleName() +
                    "\n* groupLimit=" + groupLimit +
                    "\n* limit=" + limit;
        }


        @Getter
        @RequiredArgsConstructor
        private enum Processor {

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

}
