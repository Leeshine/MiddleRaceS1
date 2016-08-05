package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.rocketmq.QueueConsumer;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.Map;

/**
 * Created by leeshine on 7/4/16.
 */

public class GetMQSpoutV2 extends BaseRichSpout {
    protected transient QueueConsumer consumer;
    protected SpoutOutputCollector collector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("topic","body"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        consumer = QueueConsumer.getInstance();
    }

    public void nextTuple() {
        MessageExt msg = null;
        try{
            msg = consumer.getMsg();
        }catch (Exception e){
        }
        if(msg == null) return;

        String topic = msg.getTopic();
        byte tp;
        if(topic.equals(RaceConfig.MqTaobaoTradeTopic))
            tp = RaceConfig.TB;
        else if(topic.equals(RaceConfig.MqTmallTradeTopic))
            tp = RaceConfig.TM;
        else tp = RaceConfig.PAY;
        collector.emit(new Values(tp,msg.getBody()));
    }
}