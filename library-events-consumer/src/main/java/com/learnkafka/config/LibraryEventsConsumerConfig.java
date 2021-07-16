package com.learnkafka.config;

import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

	@Autowired
	LibraryEventsService libraryEventsService;

	@Bean
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
		ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
		ConsumerFactory<Object, Object> kafkaConsumerFactory) {

		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setConcurrency(3);
//		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

		factory.setErrorHandler((thrownException, data) -> {
			log.info("Exception in consumerConfig is {} and the record is {} ", thrownException.getMessage(), data);
		});

		factory.setRetryTemplate(retryTemplate());

		// Recover when exhausting all retry attempts
		factory.setRecoveryCallback(context -> {
			if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
				// invoke recovery logic
				log.info("Inside the recoverable logic");

				// Show all attributes in context
				Arrays.asList(context.attributeNames())
					.forEach(attributeName -> {
						log.info("Attribute name is : {}", attributeName);
						log.info("Attribute value is : {}", context.getAttribute(attributeName));
					});

				ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");
				libraryEventsService.handleRecovery(consumerRecord);
			} else {
				log.info("Inside the non recoverable logic");
				throw new RuntimeException(context.getLastThrowable().getMessage());
			}
			return null;
		});
		return factory;
	}

	private RetryTemplate retryTemplate() {
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(simpleRetryPolicy());

		FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
		fixedBackOffPolicy.setBackOffPeriod(1000);
		retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

		return retryTemplate;
	}

	private RetryPolicy simpleRetryPolicy() {
//		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
//		simpleRetryPolicy.setMaxAttempts(3);

		Map<Class<? extends  Throwable>, Boolean> exceptionMap = new HashMap<>();
		exceptionMap.put(IllegalArgumentException.class, false);
		exceptionMap.put(RecoverableDataAccessException.class, true);

		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionMap, true);
		return simpleRetryPolicy;
	}

}
