package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by leeshine on 7/5/16.
 */

public class RatioBolt extends BaseBasicBolt {
    private double[] pcMap;
    private double[] wireMap;
    private final static int TS = RaceConfig.TS;
    private final static short PC =0;
    private final static short WIRE = 1;
    private int flag;
    private boolean end;
    private int endTime = RaceConfig.TS;
    protected  transient TairOperatorImpl tairOperator;

    private static Logger LOG = LoggerFactory.getLogger(RatioBolt.class);
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        flag = -1;
        end = false;
        pcMap = new double[1440];
        wireMap = new double[1440];
        for(int i=0; i<1440; i++){
            pcMap[i] = 0.0;
            wireMap[i] = 0.0;
        }
        tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        byte topic = tuple.getByte(0);
        byte[] body = tuple.getBinary(2);

        if(body == null) return;

        if(topic == RaceConfig.END) {
            writeTair(flag);
            end = true;
            return;
        }

        if(topic != RaceConfig.PAY)
            return;

        LOG.info("Get ratio!!");
        PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class,body);

        long time = pay.getCreateTime();
        double price = pay.getPayAmount();
        short plat = pay.getPayPlatform();
        int ts = (int)((time/1000/60)*60);

        if(plat == PC){
            pcMap[(ts-TS)/60] += price;
        }else{
            wireMap[(ts-TS)/60] += price;
        }

        /*if(end == false){
            if(flag== -1){
                flag = ts;
            }else if(flag != ts){
                writeTair(flag);
                flag = ts;
            }
        }else{
            writeTair(ts);
        }*/
    }

    public void writeTair(int ts){
        if(flag == -1) return;
        int index = (ts-TS)/60;
        double ans1 = 0.0;
        double ans2 = 0.0;
        for(int i=0; i<=index; i++){
            ans1 += pcMap[i];
            ans2 += wireMap[i];
        }
        double ratio = ans2/ans1;
        ratio = Math.round(ratio*100)/100.00;
        String key = RaceConfig.prex_ratio+ts;
        LOG.info("write key "+key);
        tairOperator.write(key,ratio);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
