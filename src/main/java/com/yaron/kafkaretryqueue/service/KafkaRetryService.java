package com.yaron.kafkaretryqueue.service;

import com.alibaba.fastjson.JSON;
import com.yaron.kafkaretryqueue.entity.RetryRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.KafkaReplyTimeoutException;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @author yangyu
 * @date 2021/4/19 23:03
 * @description
 */
@Service
public class KafkaRetryService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRetryService.class);

    /**
     * 消费失败后下一次消费的 延迟时间
     * 60s  120s  300s ...
     */
    private static final int[] RETRY_INTERVAL_SECONDS =
            {1 * 60, 2 * 60, 5 * 60, 10 * 60, 30 * 60, 1 * 60 * 60, 2 * 60 * 60};

    @Value("${spring.kafka.topics.retry}")
    private String retryTopic;

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;


    public void consumerLater(ConsumerRecord<String, String> record) {

        // 获取已经重试的次数
        int retryTimes = getRetryTimes(record);
        // 获取下次重试时间
        Date nextConsumerTime = getNextConsumerTime(retryTimes);
        // 超过重试次数，不再进行重试
        if (Objects.isNull(nextConsumerTime)){
            return;
        }

        RetryRecord retryRecord = new RetryRecord();
        retryRecord.setNextTime(nextConsumerTime.getTime());
        retryRecord.setTopic(record.topic());
        retryRecord.setRetryTimes(retryTimes);
        retryRecord.setKey(record.key());
        retryRecord.setValue(record.value());

        // 转换为字符串
        String retryStr = JSON.toJSONString(retryRecord);

        // 发送到重试队列
        kafkaTemplate.send(retryTopic,null,retryStr);
    }

    private int getRetryTimes(ConsumerRecord<String, String> record) {
        int retryTimes = -1;

        for(Header header : record.headers()){
            if(RetryRecord.KEY_RETRY_TIMES.equals(header.key())){
                ByteBuffer buffer = ByteBuffer.wrap(header.value());
                retryTimes = buffer.getInt();
            }
        }
        retryTimes++;
        return retryTimes;
    }

    private Date getNextConsumerTime(int retryTimes){

        if (RETRY_INTERVAL_SECONDS.length < retryTimes){
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND,RETRY_INTERVAL_SECONDS[retryTimes]);
        return calendar.getTime();
    }

}
