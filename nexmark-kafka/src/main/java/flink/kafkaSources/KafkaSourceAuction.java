package flink.kafkaSources;

import flink.sources.AuctionSourceFunction;
import flink.utils.AuctionSchema;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KafkaSourceAuction {
    public static void main(String[] args) throws Exception {
        System.out.println("Options for both the above setups: ");
        System.out.println("\t[--kafka-topic <topic>]");
        System.out.println("\t[--broker <broker>]");
        System.out.println("\t[--ratelist <ratelist>]");
        System.out.println();

        // Checking input parameters
        //  --broker 192.168.1.180:9092 --kafka-topic query3  --ratelist 50000_300000_1000_600000
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String broker = params.getRequired("broker");
        final String kafkaTopic = params.getRequired("kafka-topic");
        System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, broker);
        System.out.println();
        String ratelist = params.getRequired("ratelist");
        int[] numbers = Arrays.stream(ratelist.split("_")).mapToInt(Integer::parseInt).toArray();
        System.out.println(Arrays.toString(numbers));
        List<List<Integer>> auctionSrcRates = new ArrayList<>();
        for (int i = 0; i < numbers.length - 1; i += 2) {
            auctionSrcRates.add(Arrays.asList(numbers[i], numbers[i + 1]));
        }

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Auction> auctions =
                env.addSource(new AuctionSourceFunction(auctionSrcRates))
                        .name("Custom Source: Auctions")
                        .setParallelism(params.getInt("p-auction-source", 1));

        // write the filtered data to a Kafka sink
        KafkaSink<Auction> sink =
                KafkaSink.<Auction>builder()
                        .setBootstrapServers(broker)
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(kafkaTopic)
                                        .setValueSerializationSchema(new AuctionSchema())
                                        .build())
                        .build();

        auctions.sinkTo(sink);
        // run the cleansing pipeline
        env.execute("Auction Events to Kafka");
    }
}
