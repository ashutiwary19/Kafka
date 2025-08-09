package com.ashu.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

	public static void main(String[] args) {

		Properties properties = new Properties();
		String topic_name = "java_demo1";
		String group_id = "java-consumer-demo";

		// Kafka bootstrap server
		properties.setProperty("bootstrap.servers", "localhost:9092");

		// kafka batch size for sticky partition
		// In production we don't go for small batch but rather 16kb batch sizs
		properties.setProperty("batch.size", "5");
		properties.setProperty("linger.ms", "100");

		// set partition to roundrobin
//		properties.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner");

		// Producer properties
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

		//
		properties.setProperty("group.id", group_id);
		// None/earliest/latest
		// None -> fail if we don't have existing consumer group
		// Earliest -> from beginning
		// latest -> Read from now
		properties.setProperty("auto.offset.reset", "earliest");

		// Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// Subscribe to topic
		consumer.subscribe(Arrays.asList(topic_name));
		while (true) {
			log.info("Polling");
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			records.forEach(record -> {
				log.info("Key : {} - Value : {}", record.key(), record.value());
				log.info("Partition : {} - Offset : {}", record.partition(), record.offset());
			});
		}

	}
}
