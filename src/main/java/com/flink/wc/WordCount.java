package com.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: ShuaiYu_Jia
 * @Data: 2022/1/14
 * @Description:
 */

//批处理word count
public class WordCount {
    public static void main(String[] args)  throws Exception{

        //创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String inputPath = "D:\\workspace\\Intellij_IDEA\\Flink_Learing\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = executionEnvironment.readTextFile(inputPath);

        //对数据集进行处理，按照空格进行分词转开，转换成(word,1)这样的二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) //按照第一个位置的word分组
                .sum(1); //将第二个位置上的数据求和

        resultSet.print();

    }

    //自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>
    {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

            //按空格分词
            String[] words = s.split(" ");
            //遍历所有word，包成二元组输出
            for (String word:words)
            {
                collector.collect(new Tuple2<>(word, 1));
            }

        }
    }
}


