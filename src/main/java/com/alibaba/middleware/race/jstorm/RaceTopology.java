package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;

public class RaceTopology {

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumAckers(0);

        conf.setNumWorkers(4);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new GetMQSpoutV2(), 4).setNumTasks(4);
        builder.setBolt("handle",new HandleBolt(),16).setNumTasks(16).localOrShuffleGrouping("spout");
        builder.setBolt("detail",new GetDetailBolt(),16).setNumTasks(16).fieldsGrouping("handle", new Fields("order"));
        builder.setBolt("count", new CountBoltV1(),16).setNumTasks(16).fieldsGrouping("detail",new Fields("key"));
        builder.setBolt("ratio",new RatioV1(),16).setNumTasks(16).localOrShuffleGrouping("spout");
        String topologyName = RaceConfig.JstormTopologyName;

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}