package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.swing.text.html.Option;
import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	private LibraryEventsRepository libraryEventsRepository;

	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("LibraryEvent : {}", libraryEvent);

		if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
			throw new RecoverableDataAccessException("Temporary Network Issue");
		}

		switch (libraryEvent.getLibraryEventType()) {
			case NEW:
				// save
				save(libraryEvent);
				break;

			case UPDATE:
				// update
				validate(libraryEvent);
				save(libraryEvent);
				break;

			default:
				log.info("Invalid Library Event Type");
		}

	}

	private void validate(LibraryEvent libraryEvent) {
		if (libraryEvent.getLibraryEventId() == null) {
			throw new IllegalArgumentException("Library Event Id is missing");
		}

		Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());

		if (libraryEventOptional.isEmpty()) {
			throw new IllegalArgumentException("Not a valid Library Event");
		}

		log.info("Validation is successful for Library Event: {}", libraryEventOptional.get());
	}

	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);
		log.info("Successfully persisted the LibraryEvent {}", libraryEvent);
	}

	public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
		Integer key = consumerRecord.key();
		String value = consumerRecord.value();
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);

		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
			@Override public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}

			@Override public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}
		});
	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error Sending the Message and the exception is {}", ex.getMessage());
		try {
			throw ex;
		}
		catch (Throwable throwable) {
			log.error("Error in OnFailure: {}", throwable.getMessage());
		}
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message Sent Successfully for the key: {} and the value is {}, partition is {}",
			key, value, result.getRecordMetadata().partition());
	}
}
