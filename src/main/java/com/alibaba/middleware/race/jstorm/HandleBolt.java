package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.Map;

/**
 * Created by leeshine on 7/8/16.
 */
public class HandleBolt extends BaseRichBolt {
    protected OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        byte topic = tuple.getByte(0);
        byte[] body = tuple.getBinary(1);

        if(body.length==2 && body[0]==0 && body[1]==0){
            if(topic == RaceConfig.PAY){
                collector.emit(new Values(RaceConfig.END,1l,"a"));
            }
            return;
        }

        long orderId;
        if(topic == RaceConfig.PAY){
            PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class,body);
            orderId = pay.getOrderId();
            String price =  String.valueOf(pay.getPayAmount());
            String ts = String.valueOf((pay.getCreateTime()/1000/60)*60);
            collector.emit(new Values(RaceConfig.PAY,orderId,ts+price));
        }else{
            OrderMessage order = RaceUtils.readKryoObject(OrderMessage.class,body);
            orderId = order.getOrderId();
            collector.emit(new Values(topic,orderId,"a"));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("topic","order","keys"));
    }
}