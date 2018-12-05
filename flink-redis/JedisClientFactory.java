package com.xxx.bigdata.flink.util;

import redis.clients.jedis.Jedis;

/**
 * *********************************************************
 * From XD: God only helps those who help themselves
 * *********************************************************
 * Author : xd
 * Time : 2018/11/29
 * Package : com.xxx.bigdata.flink.util
 * ProjectName: quickflink
 * Describe :
 * ============================================================
 */
public class JedisClientFactory {
    private  String host;
    private  int port;

    private JedisClientFactory(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static Jedis createJedisInstance(String host,int port) {
        Jedis jedis = new Jedis(host,port);
        return jedis;
    }

    public static void main(String[] args) {
        Jedis client = JedisClientFactory.createJedisInstance("localhost",6379);
        System.out.println(client.ping());
    }
}
