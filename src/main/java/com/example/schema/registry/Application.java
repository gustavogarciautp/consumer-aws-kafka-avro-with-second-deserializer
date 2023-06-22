package com.example.schema.registry;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.example.Customer;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

@SpringBootApplication
public class Application {

    private static final CustomAvroByteReader<Customer> AVRO_BYTE_READER = new CustomAvroByteReader<>(
            Customer.getClassSchema());

    private static final String TOPIC = "customer-avro";

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            Properties properties = new Properties();
            // normal consumer
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "customer-consumer-group");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // avro part (deserializer)
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    AWSKafkaAvroDeserializer.class.getName());
            properties.setProperty(AWSSchemaRegistryConstants.AWS_REGION, "us-east-2");
            // properties.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME,
            // "my-registry");
            properties.setProperty(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                    AvroRecordType.SPECIFIC_RECORD.getName());

            properties.setProperty(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER,
                    ByteArrayDeserializer.class.getName()); // For migration fall back scenario
            properties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

            // Optional
            // properties.setProperty(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS,
            // "86400000");
            // properties.setProperty(AWSSchemaRegistryConstants.CACHE_SIZE, "10"); //
            // default value is 200

            try (final KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(properties)) {
                kafkaConsumer.subscribe(Collections.singleton(TOPIC));
                while (true) {
                    System.out.println("Polling...");
                    final ConsumerRecords<String, Object> records = kafkaConsumer.poll(Duration.ofMillis(5000));
                    Customer customer;
                    for (ConsumerRecord<String, Object> record : records) {
                        if (record.value().getClass().isArray()) {
                            customer = AVRO_BYTE_READER.deserialize((byte[]) record.value());
                        } else {
                            customer = (Customer) record.value();
                        }
                        System.out.println(customer);
                    }
                    kafkaConsumer.commitSync();
                }
            }
        };
    }
}