package com.training;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class KafkaAvroConsumer {

    private final Consumer<String, GenericRecord> consumer;
    private final String topic;

    public KafkaAvroConsumer(String kafkaServer, String schemaRegistry, String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put("schema.registry.url", schemaRegistry);
        this.consumer = new KafkaConsumer<>(properties);
    }

    public void consumer() {
        consumer.subscribe(List.of(topic));
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    Headers headers = record.headers();
                    String schemaVersion = "";
                    for (Header header : headers) {
                        if (header.key().equals("schema-version")) {
                            schemaVersion = new String(header.value(), StandardCharsets.UTF_8);
                        }
                    }
                    System.out.println("schema-version - " + schemaVersion + ": " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
