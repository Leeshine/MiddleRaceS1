package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by leeshine on 7/4/16.
 */


public class GetDetailBoltV1 extends BaseRichBolt {
    public static class OrderInfo{
        double value ;
        Byte type ;
        public OrderInfo(double v,Byte type){
            this.value = v;
            this.type = type;
        }
        public boolean decrease(double v){
            this.value = this.value-v;
            return this.value<0.001;
        }
        public Byte getType(){
            return type;
        }

    }
    private final int SIZE = 10000;

    protected OutputCollector collector;
    private transient Map<Long,List<String>> payMap;
    private transient Map<Long,OrderInfo> maps;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        maps = new HashMap<>(SIZE);
        payMap = new HashMap<>(SIZE);
    }

    public void execute(Tuple tuple) {
        byte topic = tuple.getByte(0);
        long orderId = tuple.getLong(1);

        if(topic== RaceConfig.END){
            collector.emit(new Values(0.0,"a"));
            return;
        }


        if(topic == RaceConfig.PAY){
            String keys = tuple.getString(2);
            if(maps.containsKey(orderId)){
                String ts = keys.substring(0,10);
                double price = Double.parseDouble(keys.substring(10));
                collector.emit(new Values(price,maps.get(orderId).getType()+ts));
            }else{
                if(payMap.containsKey(orderId)==false)
                    payMap.put(orderId,new ArrayList<String>());
                payMap.get(orderId).add(keys);
            }
        }else{
            double ps = tuple.getDouble(2);
            maps.put(orderId,new OrderInfo(ps,topic));
            if(payMap.containsKey(orderId)){
                for(String key : payMap.get(orderId)){
                    String ts = key.substring(0,10);
                    double price = Double.parseDouble(key.substring(10));
                    collector.emit(new Values(price,maps.get(orderId).getType()+ts));
                    if(maps.get(orderId).decrease(price))
                        maps.remove(orderId);
                }
                payMap.remove(orderId);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("price","key"));
    }
}
