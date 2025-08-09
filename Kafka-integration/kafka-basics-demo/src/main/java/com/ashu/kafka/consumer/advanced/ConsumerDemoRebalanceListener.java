package com.ashu.kafka.consumer.advanced;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoRebalanceListener {
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

		ConsumerRebalanceListenerImpl listener = new ConsumerRebalanceListenerImpl(consumer);

		// get a reference to the current thread
		final Thread mainThread = Thread.currentThread();

		// adding the shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
				consumer.wakeup();

				// join the main thread to allow the execution of the code in the main thread
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

		try {
			// subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic_name), listener);

			// poll for new data
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					log.info("Key: " + record.key() + ", Value: " + record.value());
					log.info("Partition: " + record.partition() + ", Offset:" + record.offset());

					// we track the offset we have been committed in the listener
					listener.addOffsetToTrack(record.topic(), record.partition(), record.offset());
				}

				// We commitAsync as we have processed all data and we don't want to block until
				// the next .poll() call
				consumer.commitAsync();
			}
		} catch (WakeupException e) {
			log.info("Wake up exception!");
			// we ignore this as this is an expected exception when closing a consumer
		} catch (Exception e) {
			log.error("Unexpected exception", e);
		} finally {
			try {
				consumer.commitSync(listener.getCurrentOffsets()); // we must commit the offsets synchronously here
			} finally {
				consumer.close();
				log.info("The consumer is now gracefully closed.");
			}
		}

		log.info("Exiting the application");

	}
}
