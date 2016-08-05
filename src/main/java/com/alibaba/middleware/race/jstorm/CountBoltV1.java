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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by leeshine on 7/5/16.
 */

public class CountBoltV1 extends BaseBasicBolt{
    private static final String INIT = "init";
    private String flag;
    private  double num ;
    private final double ZERO = 0.0;
    protected  transient TairOperatorImpl tairOperator;

    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf,context);
        flag = INIT;
        num = 0.0;
        tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        double price = tuple.getDouble(0);
        String key = tuple.getString(1);//tp+key

        if(key==null) return;

        if(key.equals("a")) {
            RaceConfig.ENDS = true;
            return;
        }


        if(RaceConfig.ENDS==false){
            if(flag.equals(key)){
                num = num+price;
            }else{
                if(flag.equals(INIT)){
                    flag = key;
                    num  = price;
                    return;
                }
                writeTair();
                flag = key;
                num = price;
            }
        }else{
            writeTair();
            String key1 = key.startsWith(RaceConfig.tb) ? RaceConfig.prex_taobao : RaceConfig.prex_tmall;
            key1 = key1+key.substring(1);
            tairOperator.writeAndChange(key1,price);
        }
    }

    public void writeTair(){
        if(num-ZERO < 0.001){
            return;
        }
        String key = flag.startsWith(RaceConfig.tb) ? RaceConfig.prex_taobao : RaceConfig.prex_tmall;
        key = key+flag.substring(1);
        tairOperator.writeAndChange(key,num);
        num = ZERO;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
    }
}
