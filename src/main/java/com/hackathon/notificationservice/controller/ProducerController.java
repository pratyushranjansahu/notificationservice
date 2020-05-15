package com.hackathon.notificationservice.controller;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hackathon.notificationservice.bean.ProducerBean;

@RestController
@RequestMapping("/producer")
public class ProducerController {

	private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);

	@Value("${kafka.bootstrap.servers}")
	private String kafkaBootstrapServers;

	@Value("${kafka.topic.name}")
	private String topicName;

	@PostMapping("/produceDetails")
	public String orderDetails(@RequestBody ProducerBean producerBean) {
		// sending data to kafka topic
		// set Kafka properties
		Properties kafkaProperties = setKafkaProperties();
		KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);
		sendKafkaMessage(producerBean.toString(), producer, topicName);
		return "Producer Details is  : " + producerBean.toString();
	}

	private static void sendKafkaMessage(String payload, KafkaProducer<String, String> producer, String topic) {
		logger.info("Sending Kafka message: " + payload);
		producer.send(new ProducerRecord<>(topic, payload));
	}

	private Properties setKafkaProperties() {
		Properties producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", kafkaBootstrapServers);
		producerProperties.put("acks", "all");
		producerProperties.put("retries", 0);
		producerProperties.put("batch.size", 16384);
		producerProperties.put("linger.ms", 1);
		producerProperties.put("buffer.memory", 33554432);
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return producerProperties;
	}

}
