package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaAvroProducerV2 {

    public static void main(String[] args) {
        Properties properties = new Properties();

        // normal consumer
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.put("acks", "1");
        properties.put("retries", "10");

        // avro part (deserializer)
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<String, Customer>(properties);
        String topic = "customer-avro";

        Customer customer = Customer.newBuilder()
                .setFirstName("John")
                .setLastName("Doe")
                .setAge(26)
                .setHeight(160f)
                .setWeight(90f)
                .setPhoneNumber("123-456-789")
                .setEmail("and.rds@gmail.com")
                .build();

        ProducerRecord<String,Customer> producerRecord = new ProducerRecord<>(topic, customer);

        kafkaProducer.send(producerRecord, new Callback() {

            @Override
            public void onCompletion(final RecordMetadata recordMetadata, final Exception e) {
                if(e == null){
                    System.out.println("Success!");
                    System.out.println(recordMetadata.toString());
                } else {
                    e.printStackTrace();
                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
