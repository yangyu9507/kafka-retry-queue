package com.yaron.kafkaretryqueue.listener;

import com.yaron.kafkaretryqueue.service.KafkaRetryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yangyu
 * @date 2021/4/19 23:03
 * @description
 */
@Component
public class ConsumerListener {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerListener.class);

    @Autowired
    private KafkaRetryService kafkaRetryService;

    private static int index = 0;

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @KafkaListener(topics = "${spring.kafka.topics.test}",groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String,String> record){

        try {
            // 业务处理
            logger.info("消费的消息,key:{},value{}",record.key(),record.value());
            // 每到3的倍数时,模拟重新消息
            index++;
            if (index % 3 == 0){
                throw new Exception(sdf.format(new Date()) + " ---> index : "+index+" <--- 消费异常,请重试");
            }

        }catch (Exception ex){
            logger.error(ex.getMessage());
            // 消息重试
            kafkaRetryService.consumerLater(record);
        }
    }

}
