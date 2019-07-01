package com.task.entity;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVRecord;

@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Product {

    private final int id;
    private final String name;
    private final String condition;
    private final String state;
    private final double price;

    public static Product parse(CSVRecord record) {
        return new Product(
                Integer.parseInt(record.get(0)),
                record.get(1),
                record.get(2),
                record.get(3),
                Double.parseDouble(record.get(4))
        );
    }

    public Object[] asFieldsArray() {
        return new Object[]{id, name, condition, state, price};
    }

}
