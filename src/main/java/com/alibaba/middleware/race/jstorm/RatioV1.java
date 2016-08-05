package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import java.util.Map;

/**
 * Created by leeshine on 7/5/16.
 */

public class RatioV1 extends BaseBasicBolt {
    protected  transient RatioTask task;

    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        task = RatioTask.getInstance();
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        byte topic = tuple.getByte(0);
        byte[] body = tuple.getBinary(1);

        if(body==null || topic != RaceConfig.PAY) return;

        if(body.length == 2 && body[0]==0 && body[1]==0){
            task.setEnd();
        }


        PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class,body);

        long time = pay.getCreateTime();
        double price = pay.getPayAmount();
        short plat = pay.getPayPlatform();
        int ts = (int)((time/1000/60)*60);
        task.handle(ts,price,plat);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}