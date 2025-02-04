package com.ververica.datagen;

import com.github.javafaker.Faker;
import com.ververica.models.Transaction;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

public class DataGenerator {
    private static final Faker faker = new Faker();
    private static final Random random = new Random();
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault());

    public static Transaction generateTransaction(int totalCustomers) {
        return new Transaction(
                UUID.randomUUID().toString(),
                "CST" + random.nextInt(totalCustomers),
                formatter.format(Instant.now().minusMillis(random.nextInt(100000))),
                Math.round((10 + random.nextDouble() * 990) * 100.0) / 100.0,
                faker.currency().code(),
                random.nextBoolean() ? "DEBIT" : "CREDIT",
                faker.company().name(),
         faker.address().city() + ", " + faker.address().country(),
                Double.parseDouble(faker.address().latitude()),
                Double.parseDouble(faker.address().longitude())
        );
    }
}
