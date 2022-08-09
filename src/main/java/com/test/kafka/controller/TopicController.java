package com.test.kafka.controller;

import com.test.kafka.model.Todo;
import com.test.kafka.producer.KafkaProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/topic")
@RequiredArgsConstructor
public class TopicController {

    private final KafkaProducer kafkaProducer;

    @GetMapping("/random")
    @ResponseStatus(HttpStatus.OK)
    public void generateRandomUser() {
        kafkaProducer.sendMessageToKafka("sendMessageToKafka");

        var todo = Todo.builder().id("1").todo("todo description").build();
        kafkaProducer.sendTodoToKafka(todo);
    }

}
