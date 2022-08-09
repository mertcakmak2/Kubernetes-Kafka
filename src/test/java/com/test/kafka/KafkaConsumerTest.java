package com.test.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.kafka.consumer.KafkaConsumer;
import com.test.kafka.model.Todo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@EmbeddedKafka
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaConsumerTest {

    private Producer<String, String> producer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    @SpyBean
    private KafkaConsumer kafkaConsumer;

    @Captor
    ArgumentCaptor<Todo> todoArgumentCaptor;

    @Captor
    ArgumentCaptor<String> stringArgumentCaptor;

    @Captor
    ArgumentCaptor<ConsumerRecord<String, String>> consumerRecordArgumentCaptor;

    @BeforeAll
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producer = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer()).createProducer();
    }

    @Test
    void todo_consume_from_kafka_test() throws JsonProcessingException {

        String uuid = "11111";
        var todo = Todo.builder().id(uuid).todo("test todo").build();
        String message = objectMapper.writeValueAsString(todo);
        producer.send(new ProducerRecord<>("todo-topic", 0, uuid, message));
        producer.flush();

        verify(kafkaConsumer, timeout(5000).times(1))
                .todoTopicListener(todoArgumentCaptor.capture());

        Todo todo2 = todoArgumentCaptor.getValue();
        assertNotNull(todo2);
        assertEquals("11111", todo2.getId());
        assertEquals("test todo", todo.getTodo());
    }

    @Test
    void string_consume_from_kafka_test() throws JsonProcessingException {
        String message = "message-topic-message";
        producer.send(new ProducerRecord<>("string-message-topic", objectMapper.writeValueAsString(message)));
        producer.flush();

        verify(kafkaConsumer, timeout(5000).times(1))
                .stringTopicListener(stringArgumentCaptor.capture());

        String expectedMessage = stringArgumentCaptor.getValue();
        assertNotNull(expectedMessage);
    }

    @AfterAll
    void shutdown() {
        producer.close();
    }
}
