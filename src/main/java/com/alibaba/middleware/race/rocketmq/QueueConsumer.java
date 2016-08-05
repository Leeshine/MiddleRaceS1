package com.alibaba.middleware.race.rocketmq;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueConsumer {
    private static DefaultMQPushConsumer consumer;
    private static LinkedBlockingQueue<MessageExt> queue;

    private QueueConsumer(){
        queue = new LinkedBlockingQueue<>(1000000);
        consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
        consumer.setInstanceName(RaceConfig.MetaConsumerGroup+"@"+ JStormUtils.process_pid());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        try {
            consumer.subscribe(RaceConfig.MqPayTopic, "*");
            consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
            consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
        }catch (Exception e){
        }

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        queue.put(msg);
                    }catch (Exception e){

                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        try {
            consumer.setPullThresholdForQueue(RaceConfig.QUEUE_SIZE);
            consumer.setConsumeMessageBatchMaxSize(RaceConfig.BATCH_SIZE);
            consumer.setPullBatchSize(RaceConfig.PULL_SIZE);
            consumer.setConsumeThreadMin(RaceConfig.THREAD_NUM);
            consumer.setConsumeThreadMax(RaceConfig.THREAD_NUM);
            consumer.start();
        }catch (Exception e){
        }
    }

    private static final QueueConsumer queueConsumer = new QueueConsumer();
    public static QueueConsumer getInstance(){
        return queueConsumer;
    }

    public static MessageExt getMsg(){
        MessageExt val = null;
        try{
            val = queue.take();
            return val;
        }catch (Exception e){
        }
        return null;
    }
}