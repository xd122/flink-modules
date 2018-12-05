package com.xxx.bigdata.flink.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * *********************************************************
 * From XD: God only helps those who help themselves
 * *********************************************************
 * Author : xd
 * Time : 2018/11/27
 * Package : com.xxx.bigdata.flink.util
 * ProjectName: quickflink
 * Describe :
 * ============================================================
 */
public class LineSpliter implements FlatMapFunction<String,Tuple2<String,Integer>>{

    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] tokens = s.split("\\W+");
        Tuple2<String,Integer> result = new Tuple2<String, Integer>("",0);

        for(String token:tokens) {
            if (token.length() > 0) {
                result.f0 = token;
                result.f1 = 1;
                collector.collect(result);
            }
        }
    }
}
