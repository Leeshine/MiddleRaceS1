package com.alibaba.middleware.race.Tair;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TairOperatorImpl {
    protected int namespace;
    DefaultTairManager tairManager;
    private Map<String,Double> maps;

    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        List<String> confServers = new ArrayList<String>();
        confServers.add(masterConfigServer);
        confServers.add(slaveConfigServer);

        tairManager = new DefaultTairManager();
        tairManager.setConfigServerList(confServers);

        tairManager.setGroupName(groupName);
        tairManager.init();
        this.namespace = namespace;
        maps = new HashMap<>();
    }

    public void write(Serializable key, Serializable value) {
        /*ResultCode rc = tairManager.put(namespace, key, value);
        if (rc.isSuccess()) {
            return true;
        } else if (ResultCode.VERERROR.equals(rc)){
            return false;
        } else {
            return false;
        }*/
        tairManager.put(namespace,key,value,0);
    }

    public synchronized void  writeAndChange(String key, double value){
        if(maps.containsKey(key))
            value += maps.get(key);
        double pp = Math.round(value*100)/100.00;
        tairManager.put(namespace,key,pp,0);
        maps.put(key,value);
    }

    public void close(){
        //tairManager.close();
    }
}
