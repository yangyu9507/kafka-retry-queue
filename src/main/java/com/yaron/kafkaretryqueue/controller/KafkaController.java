package com.yaron.kafkaretryqueue.controller;

import com.yaron.kafkaretryqueue.service.KafkaService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

/**
 * @author yangyu
 * @date 2021/4/19 22:55
 * @description
 */
@RestController
public class KafkaController {

    @Value("${spring.kafka.topics.test}")
    private String topic;

    @Autowired
    private KafkaService kafkaService;

    @RequestMapping("/send/{message}")
    public String sendMessage(@PathVariable("message") String message) throws ExecutionException, InterruptedException {

        ProducerRecord<String,String> record =
                new ProducerRecord<String,String>(
                        topic,
                        message
                );

        return kafkaService.sendMessage(record);
    }
}
