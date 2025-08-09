package com.ashu.kafka.producor;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
	public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

	public static void main(String[] args) {
		String topic = "consumer_topic";
		Properties properties = new Properties();
		
		// Kafka bootstrap server
		properties.setProperty("bootstrap.servers", "localhost:9092");

		// Producer properties
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// create a Producer record
		for (int i = 11; i <= 1000; i++) {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,
					"Hello World-" + i);
			
			// Send Data --> This sends data synchronouslly so next two lines is not needed
			producer.send(producerRecord);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// Tell the producer to send all data and block until done - synchronous
		producer.flush();

		// flush and close the producer
		producer.close();
	}
}
