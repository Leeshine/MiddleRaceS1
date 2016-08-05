package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
    public static final byte END  = (byte)(0);
    public static final byte TB = (byte)(1);
    public static final byte TM = (byte)(2);
    public static final byte PAY = (byte)(3);

    public static final String tb = "1";
    public static final String tm = "2";

    public static final String prex_tmall = "platformTmall_45861fykt0_";
    public static final String prex_taobao = "platformTaobao_45861fykt0_";
    public static final String prex_ratio = "ratio_45861fykt0_";


    public static final String JstormTopologyName = "45861fykt0";
    public static final String MetaConsumerGroup = "45861fykt0";
    public static final String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static final String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static final String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static final String TairConfigServer = "10.101.72.127:5198";
    public static final String TairSalveConfigServer = "10.101.72.128:5198";
    public static final String TairGroup = "group_tianchi";
    public static final Integer TairNamespace = 5948;

     public static final Integer TS = 1447171200;

    public static Boolean ENDS = false;
    public static final int QUEUE_SIZE = 1024;
    public static final int BATCH_SIZE = 256;
    public static final int PULL_SIZE = 256;
    public static final int THREAD_NUM = 4;
}
