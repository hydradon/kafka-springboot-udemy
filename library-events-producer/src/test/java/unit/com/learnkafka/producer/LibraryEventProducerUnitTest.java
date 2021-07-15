package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerUnitTest {

	@InjectMocks
	LibraryEventProducer libraryEventProducer;

	@Mock
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Spy
	ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void sendLibraryEvent2_failure() {
		Book book = Book.builder()
			.bookId(123)
			.bookAuthor("Quang")
			.bookName("Kafka Using SpringBoot")
			.build();
		LibraryEvent event = LibraryEvent.builder()
			.libraryEventId(null)
			.book(book)
			.build();

		SettableListenableFuture future = new SettableListenableFuture();
		future.setException(new RuntimeException("Exception calling Kafka."));

		when(kafkaTemplate.send(isA(ProducerRecord.class)))
			.thenReturn(future);

		assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEvent2(event).get());
	}

	@Test
	public void sendLibraryEvent2_success() throws JsonProcessingException, ExecutionException, InterruptedException {
		// given
		Book book = Book.builder()
			.bookId(123)
			.bookAuthor("Quang")
			.bookName("Kafka Using SpringBoot")
			.build();
		LibraryEvent event = LibraryEvent.builder()
			.libraryEventId(null)
			.book(book)
			.build();

		SettableListenableFuture future = new SettableListenableFuture();

		ProducerRecord<Integer, String> producerRecord =
			new ProducerRecord("library-events", event.getLibraryEventId(), objectMapper.writeValueAsString(event));
		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342, System.currentTimeMillis(), 1, 2);

		SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
		future.set(sendResult);

		// when
		when(kafkaTemplate.send(isA(ProducerRecord.class)))
			.thenReturn(future);

		ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventProducer.sendLibraryEvent2(event);

		// then
		SendResult<Integer, String> sendResult1 = listenableFuture.get();
		assertTrue(sendResult1.getRecordMetadata().partition() == 1);
	}
}
