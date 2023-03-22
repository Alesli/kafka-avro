package com.training;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

public class KafkaAvroProducer {

    private final KafkaProducer<String, Customer> kafkaProducer;
    private final String topic;
    private final SchemaRegistryClient schemaRegistryClient;

    public KafkaAvroProducer(String kafkaServer, String schemaRegistry, String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put("schema.registry.url", schemaRegistry);
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistry, 10);
        kafkaProducer = new KafkaProducer<>(properties);
    }

    public void producer() {
        Customer customer = Customer.newBuilder()
                .setFirstName("Alex")
                .setLastName("Doe")
                .setAge(38)
                .setHeight(182.7f)
                .setTime(System.currentTimeMillis())
                .build();
        try {
            ProducerRecord<String, Customer> customerRecord = new ProducerRecord<>(topic, "key", customer);
            Headers headers = customerRecord.headers();
            Integer version = schemaRegistryClient.getVersion(topic + "-value", customer.getSchema());
            headers.add(new RecordHeader("schema-version", String.valueOf(version).getBytes()));

            kafkaProducer.send(customerRecord);
        } catch (SerializationException | RestClientException | IOException exception) {
            exception.printStackTrace();
        }
    }

    public void shutDown() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
