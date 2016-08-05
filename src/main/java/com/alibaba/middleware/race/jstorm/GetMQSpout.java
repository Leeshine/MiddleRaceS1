package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.QueueConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by leeshine on 7/4/16.
 */
public class GetMQSpout extends BaseRichSpout {
    protected SpoutOutputCollector collector;
    private transient DefaultMQPushConsumer consumer;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("topic","body"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
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
                    String topic = msg.getTopic();
                    byte tp;
                    if(topic.equals(RaceConfig.MqTaobaoTradeTopic))
                        tp = RaceConfig.TB;
                    else if(topic.equals(RaceConfig.MqTmallTradeTopic))
                        tp = RaceConfig.TM;
                    else tp = RaceConfig.PAY;
                    collector.emit(new Values(tp,msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.setPullThresholdForQueue(RaceConfig.QUEUE_SIZE);
        consumer.setConsumeMessageBatchMaxSize(RaceConfig.BATCH_SIZE);
        consumer.setPullBatchSize(RaceConfig.PULL_SIZE);
        consumer.setConsumeThreadMin(RaceConfig.THREAD_NUM);
        consumer.setConsumeThreadMax(RaceConfig.THREAD_NUM);
        try {
            consumer.start();
        }catch (Exception e){

        }
    }

    public void nextTuple() {
    }
}