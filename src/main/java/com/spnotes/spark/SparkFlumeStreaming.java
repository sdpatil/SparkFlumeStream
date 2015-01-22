package com.spnotes.spark;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

import java.util.Arrays;

/**
 * Created by gpzpati on 1/20/15.
 */
public class SparkFlumeStreaming {

    public static void main(String[] argv){
        System.out.println("Entering SparkFlumeStreaming.main");
        if(argv.length !=2 ){
            System.err.println("Usage: SparkFlumeStreaming <host> <port>");
            System.exit(1);
        }

        String hostName = argv[0];
        int port = Integer.parseInt(argv[1]);
        System.out.println("Listen as flume server on" + hostName +":"+port);


        SparkConf sparkConf = new SparkConf().setAppName("SparkFlumeStreaming");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));


        JavaReceiverInputDStream<SparkFlumeEvent> flumeEventStream = FlumeUtils.createStream(javaStreamingContext, hostName, port);

        flumeEventStream.flatMap(new FlatMapFunction<SparkFlumeEvent, String>() {
            @Override
            public Iterable<String> call(SparkFlumeEvent sparkFlumeEvent) throws Exception {
                System.out.println("Inside flatMap.call() ");
                Schema  avroSchema = sparkFlumeEvent.event().getSchema();
                System.out.println("Avro schema " + avroSchema.toString(true));
                DatumReader<GenericRecord> genericRecordReader = new GenericDatumReader<GenericRecord>(avroSchema);
                byte[] bodyArray = sparkFlumeEvent.event().getBody().array();
               System.out.println("Message " +new String(sparkFlumeEvent.event().getBody().array()));

                return Arrays.asList("Hello World".split(" "));
            }
        }).print();

        flumeEventStream.count();

        flumeEventStream.count().map(new Function<Long, String>() {
            @Override
            public String call(Long in) {
                return "Received " + in + " flume events.";
            }
        }).print();


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

        System.out.println("Exiting SparkFlumeStreaming.main");
    }
}
