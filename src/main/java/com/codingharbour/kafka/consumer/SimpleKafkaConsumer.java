package com.codingharbour.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {

    public static void main(String[] args) {
        //create kafka consumer

        /* Setup Loggin */
        Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients.consumer.internals.Fetcher", "debug");

        Properties properties = new Properties();
        String CLIENT_RACK_ID=System.getenv("CLIENT_RACK_ID");
        System.out.println(CLIENT_RACK_ID);

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-internal-service:9094");
        properties.put(ConsumerConfig.CLIENT_RACK_CONFIG,CLIENT_RACK_ID);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to topic
        consumer.subscribe(Collections.singleton("topic3"));

        //poll the record from the topic
        while (true) {
            final int giveUp = 100;   int noRecordsCount = 0;
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));

            if (records.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            records.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("Done");
    }
}
