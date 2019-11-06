package com.task;

import com.task.entity.Product;
import com.task.pipeline.EntitiesPipeline;
import com.task.pipeline.consumer.ToCsvFileConsumer;
import com.task.pipeline.producer.FromDirCsvFilesProducer;

import java.util.Comparator;

public class Runner {

    public static void main(String[] args) {
        EntitiesPipeline<Product> pipeline = configurePipeline(Configuration.parse(args));
        System.out.println("Processing...");
        try {
            pipeline.execute();
            System.out.println("Processed successfully");
        } catch (Exception e) {
            System.out.println("Processing error: " + e);
            System.exit(1);
        }
    }

    private static EntitiesPipeline<Product> configurePipeline(Configuration configuration) {
        return new EntitiesPipeline<>(
                FromDirCsvFilesProducer.withDefaultFormat(Product::parse, configuration.getInputDir()),
                configuration.getProcessor().instantiate(
                        Product::getId,
                        Comparator.comparingDouble(Product::getPrice)
                                .thenComparingInt(Product::getId)
                                .thenComparing(Product::getCondition)
                                .thenComparing(Product::getState),
                        configuration.getGroupLimit(), configuration.getLimit()),
                ToCsvFileConsumer.withDefaultFormat(Product::asFieldsArray, configuration.getOutputFile()));
    }

}
