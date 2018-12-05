package com.xxx.bigdata.flink;

import com.xxx.bigdata.flink.sink.RedisDemoMapper;
import com.xxx.bigdata.flink.sink.RedisPvSink;
import com.xxx.bigdata.flink.util.JedisClientFactory;
import com.xxx.bigdata.flink.util.LineSpliter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * *********************************************************
 * From XD: God only helps those who help themselves
 * *********************************************************
 * Author : xd
 * Time : 2018/11/28
 * Package : com.xxx.bigdata.flink
 * ProjectName: quickflink
 * Describe :
 * ============================================================
 */
public class FlinkRedisMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment stream = StreamExecutionEnvironment.getExecutionEnvironment();
//        SingleOutputStreamOperator<Tuple2<String, String>> data = stream.readTextFile("/Users/xd/Documents/scala_reaserch_file/a")
//                .flatMap(new LineSpliter()).keyBy(0).sum(1).setParallelism(2).map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, String>>() {
//
//                    @Override
//                    public Tuple2<String, String> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                        return new Tuple2<>(stringIntegerTuple2.f0,stringIntegerTuple2.f1.toString());
//                    }
//                });

//        data.print();
//        stream.execute("Start...");

//        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        SingleOutputStreamOperator<String> data = stream.readTextFile("/Users/xd/Documents/scala_reaserch_file/a")
                .flatMap(new LineSpliter()).keyBy(0).sum(1).setParallelism(2).map(new MapFunction<Tuple2<String, Integer>, String>() {
                    private static final long serialVersionUID = 495832856713948499L;

                    @Override
                    public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0 + stringIntegerTuple2.f1.toString();
                    }
                });

//        data.addSink(new RedisSink<Tuple2<String, String>>(config,new RedisDemoMapper()));
        data.addSink(new RedisPvSink(JedisClientFactory.createJedisInstance("localhost",6379)));

        stream.execute("Start...");

    }
}
