package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
								  "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;

	@Autowired
	ObjectMapper objectMapper;

	@SpyBean
	LibraryEventsConsumer libraryEventsConsumerSpy;

	@SpyBean
	LibraryEventsService libraryEventsServiceSpy;

	@Autowired
	LibraryEventsRepository libraryEventsRepository;

	@BeforeEach
	public void setup() {
		for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}

	@AfterEach
	void tearDown() {
		libraryEventsRepository.deleteAll();
	}

	@Test
	public void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
		// given
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using SpringBoot\",\"bookAuthor\":\"Quang\"}}";
		kafkaTemplate.sendDefault(json).get();

		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		// then
		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		List<LibraryEvent> eventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
		assertEquals(1, eventList.size());
		eventList.forEach(libraryEvent -> {
			assertNotNull(libraryEvent);
			assertEquals(456, libraryEvent.getBook().getBookId());
		});
	}

	@Test
	public void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
		// Save the new LibraryEvent
		String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using SpringBoot\",\"bookAuthor\":\"Quang\"}}";
		LibraryEvent event = objectMapper.readValue(json, LibraryEvent.class);
		event.getBook().setLibraryEvent(event);
		libraryEventsRepository.save(event);

		// given
		Book updatedBook = Book.builder()
			.bookId(456)
			.bookName("Kafka Using Spring Boot 2")
			.bookAuthor("Quang2")
			.build();
		event.setBook(updatedBook);
		event.setLibraryEventType(LibraryEventType.UPDATE);

		String updatedJson = objectMapper.writeValueAsString(event);
		kafkaTemplate.sendDefault(event.getLibraryEventId(), updatedJson).get();

		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		// then
		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		LibraryEvent persistedEvent = libraryEventsRepository.findById(event.getLibraryEventId()).get();
		assertEquals("Kafka Using Spring Boot 2", persistedEvent.getBook().getBookName());
		assertEquals("Quang2", persistedEvent.getBook().getBookAuthor());

	}

	@Test
	public void publishUpdateLibraryEvent_invalidEvent_nonExistentID() throws JsonProcessingException, ExecutionException, InterruptedException {
		// given
		Integer libraryEventId = 123;
		String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using SpringBoot\",\"bookAuthor\":\"Quang\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();

		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		// then
		verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

		Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
		assertFalse(libraryEventOptional.isPresent());
	}

	@Test
	public void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
		//given
		Integer libraryEventId = null;
		String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
	}

	@Test
	public void publishModifyLibraryEvent_000_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
		//given
		Integer libraryEventId = 000;
		String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		//when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

//		verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));

		// call 4 times = 3 times recovery + 1 times after recovery
		verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));

		verify(libraryEventsServiceSpy, times(1)).handleRecovery(isA(ConsumerRecord.class));
	}
}
