package com.yaron.kafkaretryqueue.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

/**
 * @author yangyu
 * @date 2021/4/19 22:56
 * @description
 */
@Service
public class KafkaService {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    public String sendMessage(ProducerRecord<String,String> record) throws ExecutionException, InterruptedException {

        SendResult<String, String> sendResult = this.kafkaTemplate.send(record).get();

        RecordMetadata metadata = sendResult.getRecordMetadata();
        String topic = metadata.topic();
        int partition = metadata.partition();
        long offset = metadata.offset();

        String res = String.format("发送消息成功: 主题 : %s,消息: %s ",topic,record.value());
        System.out.println(res);
        return res;
    }
}
