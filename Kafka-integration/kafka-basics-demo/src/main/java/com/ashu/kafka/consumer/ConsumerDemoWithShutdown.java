package com.ashu.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithShutdown {
	public static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

	public static void main(String[] args) {

		Properties properties = new Properties();
		String topic_name = "java_demo1";
		String group_id = "java-consumer-demo-2";

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

		// get a reference to the current thread
		final Thread main = Thread.currentThread();

		// adding shut down hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.info("VM Shutdown detected, let's exit by calling consumer.wakup()...");
				consumer.wakeup();

				// join the main thread to allow the execution of the code in main thread
				try {
					main.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

		try {
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
		} catch (WakeupException e) {
			log.info("Consumer is starting to shutdown");
		} catch (Exception e) {
			log.error("Unexpected exception occured :", e);
		} finally {
			consumer.close();
			log.info("The consumer is now gracefully shut down");
		}

	}
}
