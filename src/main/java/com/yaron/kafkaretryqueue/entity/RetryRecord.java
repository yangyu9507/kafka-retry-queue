package com.yaron.kafkaretryqueue.entity;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yangyu
 * @date 2021/4/19 23:09
 * @description
 */
public class RetryRecord {

    public static final String KEY_RETRY_TIMES = "retryTimes";

    private String key;
    private String value;

    private Integer retryTimes;
    private String topic;
    private Long nextTime;

    public ProducerRecord<String,String> parse(){
        Integer partition = null;
        Long timestamp = System.currentTimeMillis();
        List<Header> headers = new ArrayList<>();
        // java.nio包公开了Buffer API，使得Java程序可以直接控制和运用缓冲区
        ByteBuffer retryTimesBuffer = ByteBuffer.allocate(Integer.BYTES);
        retryTimesBuffer.putInt(retryTimes);
        // 翻转就是将一个处于存数据状态的缓冲区变为一个处于准备取数据的状态
        retryTimesBuffer.flip();
        headers.add(new RecordHeader(RetryRecord.KEY_RETRY_TIMES,retryTimesBuffer));

        return new ProducerRecord<>(
                        topic,
                        partition,
                        timestamp,
                        key,
                        value,
                        headers
                );
    }

    public RetryRecord() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(Integer retryTimes) {
        this.retryTimes = retryTimes;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Long getNextTime() {
        return nextTime;
    }

    public void setNextTime(Long nextTime) {
        this.nextTime = nextTime;
    }
}
