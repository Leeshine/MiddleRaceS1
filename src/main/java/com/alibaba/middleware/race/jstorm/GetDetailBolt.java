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


public class GetDetailBolt extends BaseRichBolt {
    protected OutputCollector collector;
    private transient Map<Long,List<String>> payMap;
    private transient Map<Long,Byte> maps;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        maps = new HashMap<>();
        payMap = new HashMap<>();
    }

    public void execute(Tuple tuple) {
        byte topic = tuple.getByte(0);
        long orderId = tuple.getLong(1);
        String keys = tuple.getString(2);

        if(topic== RaceConfig.END){
            collector.emit(new Values(0.0,"a"));
            return;
        }


        if(topic == RaceConfig.PAY){
            if(maps.containsKey(orderId)){
                String ts = keys.substring(0,10);
                double price = Double.parseDouble(keys.substring(10));
                collector.emit(new Values(price,maps.get(orderId)+ts));
            }else{
                if(payMap.containsKey(orderId)==false)
                    payMap.put(orderId,new ArrayList<String>());
                payMap.get(orderId).add(keys);
            }
        }else{
            maps.put(orderId,topic);
            if(payMap.containsKey(orderId)){
                for(String key : payMap.get(orderId)){
                    String ts = key.substring(0,10);
                    double price = Double.parseDouble(key.substring(10));
                    collector.emit(new Values(price,maps.get(orderId)+ts));
                }
                payMap.remove(orderId);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("price","key"));
    }
}