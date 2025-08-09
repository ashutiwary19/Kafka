package com.ashu.kafka.producor;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

	public static void main(String[] args) {
		String topic = "java_demo1";
		Properties properties = new Properties();

		// Kafka bootstrap server
		properties.setProperty("bootstrap.servers", "localhost:9092");

		// kafka batch size for sticky partition
		// In production we don't go for small batch but rather 16kb batch sizs
		properties.setProperty("batch.size", "1000");
		properties.setProperty("linger.ms", "100");

		// set partition to roundrobin
		properties.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner");

		// Producer properties
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// create a Producer record
		for (int j = 1; j <= 100; j++) {
			for (int i = 1; i <= 100; i++) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,
						"Producer with callback demo : " + j + "-" + i);

				// Send Data --> This sends data synchronouslly so next two lines is not needed
				producer.send(producerRecord, new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception == null) {
							log.info("Received new Metadata \n Topic : {}\nPartition : {}\nOffset : {}\nTimestamp : {}",
									metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
						} else {
							log.error("Exception occured : ", exception);
						}
					}

				});
				producer.flush();

				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		// Tell the producer to send all data and block until done - synchronous

		// flush and close the producer
		producer.close();
	}
}
