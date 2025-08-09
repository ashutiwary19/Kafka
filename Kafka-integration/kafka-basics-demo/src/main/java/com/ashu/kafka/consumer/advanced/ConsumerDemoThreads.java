package com.ashu.kafka.consumer.advanced;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {

	public static void main(String[] args) {
		ConsumerDemoWorker consumerDemoWorker = new ConsumerDemoWorker();
		new Thread(consumerDemoWorker).start();
		Runtime.getRuntime().addShutdownHook(new Thread(new ConsumerDemoCloser(consumerDemoWorker)));
	}

	private static class ConsumerDemoWorker implements Runnable {

		private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWorker.class);

		private CountDownLatch countDownLatch;
		private Consumer<String, String> consumer;

		@Override
		public void run() {
			countDownLatch = new CountDownLatch(1);
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
//			properties.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner");

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
//			properties.setProperty("group.instance.id", "...");
//			properties.setProperty("session.timeout.ms", "...");

			// create consumer
			consumer = new KafkaConsumer<String, String>(properties);

			consumer.subscribe(Collections.singleton("demo_java"));

			final Duration pollTimeout = Duration.ofMillis(100);

			try {
				while (true) {
					final ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
					for (final ConsumerRecord<String, String> consumerRecord : consumerRecords) {
						log.info("Getting consumer record key: '" + consumerRecord.key() + "', value: '"
								+ consumerRecord.value() + "', partition: " + consumerRecord.partition()
								+ " and offset: " + consumerRecord.offset() + " at "
								+ new Date(consumerRecord.timestamp()));
					}
				}
			} catch (WakeupException e) {
				log.info("Consumer poll woke up");
			} finally {
				consumer.close();
				countDownLatch.countDown();
			}
		}

		void shutdown() throws InterruptedException {
			consumer.wakeup();
			countDownLatch.await();
			log.info("Consumer closed");
		}

	}

	private static class ConsumerDemoCloser implements Runnable {

		private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCloser.class);

		private final ConsumerDemoWorker consumerDemoWorker;

		ConsumerDemoCloser(final ConsumerDemoWorker consumerDemoWorker) {
			this.consumerDemoWorker = consumerDemoWorker;
		}

		@Override
		public void run() {
			try {
				consumerDemoWorker.shutdown();
			} catch (InterruptedException e) {
				log.error("Error shutting down consumer", e);
			}
		}
	}
}