package com.test.kafka.producer;

import com.test.kafka.model.Todo;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTodoToKafka(Todo todo) {
        kafkaTemplate.send("todo-topic", todo.getId(), todo);
    }

    public void sendMessageToKafka(String message) {
        kafkaTemplate.send("string-message-topic", message, message);
    }

}
