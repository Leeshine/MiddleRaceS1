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
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Map;

/**
 * Created by leeshine on 7/4/16.
 */
public class GetMQSpoutV1 extends BaseRichSpout {
    protected SpoutOutputCollector collector;
    private transient DefaultMQPushConsumer consumer;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("topic","order","body"));
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

                    byte [] body = msg.getBody();
                    String topic = msg.getTopic();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        if(topic.equals(RaceConfig.MqPayTopic)) {
                                collector.emit(new Values(RaceConfig.END,1l,body));
                        }
                        continue;
                    }
                    long orderId;
                    if(topic.equals(RaceConfig.MqPayTopic)){
                        PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class,body);
                        orderId = pay.getOrderId();
                        collector.emit(new Values(RaceConfig.PAY,orderId,body));
                    }else{
                        OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class,body);
                        orderId = order.getOrderId();
                        Values val;
                        if(topic.equals(RaceConfig.MqTaobaoTradeTopic)){
                            val = new Values(RaceConfig.TB,orderId,new byte[]{0});
                        }else{
                            val = new Values(RaceConfig.TM,orderId,new byte[]{0});
                        }
                        collector.emit(val);
                    }

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