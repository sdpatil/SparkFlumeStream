package com.spnotes.spark;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.streaming.Durations;

import java.util.Arrays;

/**
 * Created by gpzpati on 1/21/15.
 */
public class SparkNetworkStreaming {

    public static void main(String[] argv){
        SparkConf sparkConf = new SparkConf().setAppName("NetworkWordCount");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost",9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        JavaPairDStream<String, Integer> wordCountMap = wordMap.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer first, Integer second) throws Exception {
                        return first + second;
                    }
                }
        );

        wordCountMap.print();

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
