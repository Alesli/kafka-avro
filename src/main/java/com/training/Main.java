package com.training;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) {

        String topic = "customer-registration";
        String kafkaServer = "http://localhost:9092";
        String schemaRegistry = "http://localhost:8081";

        KafkaAvroProducer kafkaAvroProducer = new KafkaAvroProducer(kafkaServer, schemaRegistry, topic);
        KafkaAvroConsumer kafkaAvroConsumer = new KafkaAvroConsumer(kafkaServer, schemaRegistry, topic);

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        try {
            executor.scheduleAtFixedRate(kafkaAvroProducer::producer, 0, 1, TimeUnit.SECONDS);
            kafkaAvroProducer.producer();
            kafkaAvroConsumer.consumer();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            kafkaAvroProducer.shutDown();
        }
    }
}
