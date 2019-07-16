package com.task;

import com.task.entity.Product;
import com.task.pipeline.EntitiesPipeline;
import com.task.pipeline.consumer.ToCsvFileConsumer;
import com.task.pipeline.processor.GroupingSortingLimitingProcessor;
import com.task.pipeline.producer.FromDirCsvFilesProducer;

import java.nio.file.Paths;
import java.util.Comparator;

public class Runner {

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            return;
        }
        EntitiesPipeline<Product> pipeline = defaultPipeline(args[0], args[1]);
        System.out.println("Processing...");
        try {
            pipeline.execute();
            System.out.println("Processed successfully");
        } catch (Exception e) {
            System.out.println("Processing error: " + e);
        }
    }

    private static EntitiesPipeline<Product> defaultPipeline(String inputDir, String outputFile) {
        return new EntitiesPipeline<>(
                FromDirCsvFilesProducer.withDefaultFormat(Product::parse, Paths.get(inputDir)),
                GroupingSortingLimitingProcessor.withDefaultLimits(
                        Product::getId,
                        Comparator.comparing(Product::getPrice)
                                .thenComparing(Product::getId)
                                .thenComparing(Product::getCondition)
                                .thenComparing(Product::getState)),
                ToCsvFileConsumer.withDefaultFormat(Product::asFieldsArray, Paths.get(outputFile)));
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar <jar_name> <input_dir_path> <output_file_path>");
    }

}
