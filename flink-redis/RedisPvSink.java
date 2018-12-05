package com.xxx.bigdata.flink.sink;

import com.xxx.bigdata.flink.util.JedisClientFactory;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * *********************************************************
 * From XD: God only helps those who help themselves
 * *********************************************************
 * Author : xd
 * Time : 2018/11/29
 * Package : com.xxx.bigdata.flink.sink
 * ProjectName: quickflink
 * Describe :
 * ============================================================
 */
public class RedisPvSink extends RichSinkFunction<String> {

    transient private Jedis jedis = null;

    public RedisPvSink(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public void invoke(String s) throws Exception {
        if (null != jedis) {
            jedis.incr(s);
        } else {
            Jedis jedis = JedisClientFactory.createJedisInstance("localhost",6379);
            jedis.incr(s);
        }
    }
}
