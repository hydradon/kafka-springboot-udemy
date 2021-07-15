package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.swing.text.html.Option;
import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	private LibraryEventsRepository libraryEventsRepository;

	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("LibraryEvent : {}", libraryEvent);

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
}
