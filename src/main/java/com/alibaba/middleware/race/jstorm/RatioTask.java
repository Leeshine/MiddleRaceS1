package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;

/**
 * Created by leeshine on 7/8/16.
 */
public class RatioTask {
    private final int CNT = 1440;
    protected  transient TairOperatorImpl tairOperator;
    private final static short PC =0;
    private final static short WIRE = 1;

    private double[] pcMap;
    private double[] wireMap;

    private static Boolean isEnd = false;
    private static int flag = -1;

    private RatioTask(){
        tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        flag = -1;
        isEnd = false;
        pcMap = new double[CNT+1];
        wireMap = new double[CNT+1];
        for(int i=0; i<=CNT; i++){
            pcMap[i] = 0.0;
            wireMap[i] = 0.0;
        }
    }

    private int lowBit(int i){
        return i &(i^(i-1));
    }

    private  double getSum(int i, short ps){
        double ans = 0;
        while(i >= 1){
            if(ps == PC){
                ans += pcMap[i];
            }else{
                ans += wireMap[i];
            }
             i = i-lowBit(i);
        }
        return ans;
    }

    private  void update(int i, double val, short ps){
        while(i <= CNT){
            if(ps == PC){
                pcMap[i] += val;
            }else wireMap[i] += val;
            i = i+lowBit(i);
        }
    }



    private  static RatioTask task = new RatioTask();

    public static RatioTask getInstance(){
        return task;
    }

    public synchronized void handle(int ts, double price, short plat){
        int index = (ts-RaceConfig.TS)/60+1;
        update(index,price,plat);

        if(isEnd == false) {
            if (flag == -1){
                flag = ts;
            } else if (flag != ts){
                writeTair(flag);
                flag = ts;
            }
        }else{
            writeTair(ts);
        }
    }

    public void setEnd(){
        isEnd = true;
    }

    public void writeTair(int ts){
        int in = (ts-RaceConfig.TS)/60+1;

        double ans1 = getSum(in,PC);
        double ans2 = getSum(in,WIRE);

        double ratio = ans2/ans1;
        ratio = Math.round(ratio*100)/100.00;
        String key = RaceConfig.prex_ratio+ts;
        tairOperator.write(key,ratio);
    }
}
