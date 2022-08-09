package com.test.kafka.consumer;

import com.test.kafka.model.Todo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class KafkaConsumer {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "todo-topic",
            //concurrency = "${spring.kafka.consumer.level.concurrency:3}",
            properties = {"spring.json.value.default.type=com.test.kafka.model.Todo"})
    public void todoTopicListener(Todo todo) {
        logger.info("Received a todo message {}, date {}", todo.getTodo(), new Date());
    }

    @KafkaListener(topics = "string-message-topic",
            //concurrency = "${spring.kafka.consumer.level.concurrency:3}",
            properties = {"spring.json.value.default.type=java.lang.String"})
    public void stringTopicListener(String message) {
        logger.info("Received a string message {}, date {}", message, new Date());
    }

}
