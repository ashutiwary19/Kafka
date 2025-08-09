package com.ashu.kafka.consumer.advanced;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
	public static void main(String[] args) {

		Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

		// create consumer configs
		Properties properties = new Properties();
		String topic_name = "java_demo1";
		String group_id = "java-consumer-demo-2";

		// Kafka bootstrap server
		properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		// kafka batch size for sticky partition
		// In production we don't go for small batch but rather 16kb batch sizs
		properties.setProperty("batch.size", "5");
		properties.setProperty("linger.ms", "100");

		// set partition to roundrobin
//		properties.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner");

		// Producer properties
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		//
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
		// None/earliest/latest
		// None -> fail if we don't have existing consumer group
		// Earliest -> from beginning
		// latest -> Read from now
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// For adding coperative/increment assignment when rebalancing
		properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
				CooperativeStickyAssignor.class.getName());
		// strategy for static assignments
//		properties.setProperty("group.instance.id", "...");
//		properties.setProperty("session.timeout.ms", "...");

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// assign and seek are mostly used to replay data or fetch a specific message

		// assign
		TopicPartition partitionToReadFrom = new TopicPartition(topic_name, 0);
		long offsetToReadFrom = 7L;
		consumer.assign(Arrays.asList(partitionToReadFrom));

		// seek
		consumer.seek(partitionToReadFrom, offsetToReadFrom);

		int numberOfMessagesToRead = 5;
		boolean keepOnReading = true;
		int numberOfMessagesReadSoFar = 0;

		// poll for new data
		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				numberOfMessagesReadSoFar += 1;
				log.info("Key: " + record.key() + ", Value: " + record.value());
				log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
				if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
					keepOnReading = false; // to exit the while loop
					break; // to exit the for loop
				}
			}
		}

		log.info("Exiting the application");

	}
}