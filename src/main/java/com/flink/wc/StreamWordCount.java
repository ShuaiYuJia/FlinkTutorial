package com.flink.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: ShuaiYu_Jia
 * @Data: 2022/1/14
 * @Description:
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception{

        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取(一行一行读取，相当于有界流)
//        String inputPath = "D:\\workspace\\Intellij_IDEA\\Flink_Learing\\FlinkTutorial\\src\\main\\resources\\hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        //



        //基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        resultStream.print();

        //执行任务
        env.execute();
    }
}
