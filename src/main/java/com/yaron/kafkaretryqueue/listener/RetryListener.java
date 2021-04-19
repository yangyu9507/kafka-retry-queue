package com.yaron.kafkaretryqueue.listener;

import com.alibaba.fastjson.JSON;
import com.yaron.kafkaretryqueue.entity.RetryRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;
import java.util.UUID;

/**
 * @author yangyu
 * @date 2021/4/19 23:36
 * @description
 */
@RestController
public class RetryListener {

    private Logger logger = LoggerFactory.getLogger(RetryListener.class);

    private static final String RETRY_KEY_ZET = "_retry_key";
    private static final String RETRY_VALUE_MAP = "_retry_value";

    @Autowired
    private RedisTemplate<String,Object> redisTemplate;
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @Value("${spring.kafka.topics.test}")
    private String bizTopic;

    @KafkaListener(topics = "${spring.kafka.topics.retry}")
    public void consume(ConsumerRecord<String,String> record){
        logger.info("放到 redis 需要重试的消息: "+ record.value());

        RetryRecord retryRecord = JSON.parseObject(record.value(),RetryRecord.class);

        /**
         * 防止待重试消息太多 撑爆redis, 可以将待 重试的消息按下一次重试时间分开
         * 存储放到不同介质
         *
         * 例如 下一次重试时间在 30min 之后的消息储存在mysql, 并定时从mysql读取即将重试的消息
         * 存储到redis中
         */
        // 通过 redis的zset进行排序
        String key = UUID.randomUUID().toString();
        redisTemplate.opsForZSet().add(RETRY_KEY_ZET,key,Double.parseDouble(String.valueOf(retryRecord.getNextTime())));
        redisTemplate.opsForHash().put(RETRY_VALUE_MAP,key,record.value());



    }

    @Scheduled(fixedDelay = 3000)
//    @RequestMapping("/retry")
    public void retryFromRedis(){
        logger.warn("Retry From Redis --- begin ---");
        long currentTime = System.currentTimeMillis();

        // 根据时间倒序获取
        Set<ZSetOperations.TypedTuple<Object>> typedTuples = redisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(RETRY_KEY_ZET, 0, currentTime);

        // 移除取出的消息
        redisTemplate.opsForZSet().removeRangeByScore(RETRY_KEY_ZET,0,currentTime);

        for (ZSetOperations.TypedTuple<Object> tuple: typedTuples){
            String key = tuple.getValue().toString();

            String value = redisTemplate.opsForHash().get(RETRY_VALUE_MAP, key).toString();

            redisTemplate.opsForHash().delete(RETRY_VALUE_MAP,key);

            RetryRecord retryRecord = JSON.parseObject(value, RetryRecord.class);
            ProducerRecord<String, String> parseRecord =
                    retryRecord.parse();

            /**
             * parseRecord.timestamp() 将会在指定 的时间 进行消息的发送
             */
            ProducerRecord recordReal =
                    new ProducerRecord(
                            bizTopic,
                            parseRecord.partition(),
                            parseRecord.timestamp(),
                            parseRecord.key(),
                            parseRecord.value(),
                            parseRecord.headers()
                    );
            kafkaTemplate.send(recordReal);
            //TODO 发生异常将发送失败的消息重新发送到redis中
        }

    }


}
