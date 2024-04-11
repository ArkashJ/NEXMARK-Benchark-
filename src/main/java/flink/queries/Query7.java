package flink.queries;

// Bid, Auction, Person imports
import flink.sinks.DummyLatencyCountingSink;
import flink.sources.AuctionSourceFunction;
import flink.sources.PersonSourceFunction;
import flink.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// kafka imports
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import java.util.*;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import flink.utils.AuctionSchema;
import flink.utils.BidSchema;
import flink.utils.PersonSchema;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.DateTime;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


import java.time.Duration;

public class Query7{
    private static final Logger logger = LoggerFactory.getLogger(Query7.class);
    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String Broker = params.getRequired("broker");
        final String auctionKafkaTopic = params.getRequired("auction-kafka-topic");
        final String auctionKafkaGroup = params.getRequired("auction-kafka-group");
        final String personKafkaTopic = params.getRequired("person-kafka-topic");
        final String personKafkaGroup = params.getRequired("person-kafka-group");
        final String bidKafkaTopic = params.getRequired("bid-kafka-topic");
        final String bidKafkaGroup = params.getRequired("bid-kafka-group");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // NOTE: set checkpointing intervals
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.getConfig().setLatencyTrackingInterval(5000);
        env.setParallelism(4);

        KafkaSource<Person> personKafkaSource =
                KafkaSource.<Person>builder()
                        .setBootstrapServers(Broker)
                        .setGroupId(personKafkaGroup)
                        .setTopics(personKafkaTopic)
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(new PersonSchema()))
                        .setProperty(
                                KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();
        DataStream<Person> persons = env.fromSource(
                personKafkaSource,
                WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.dateTime),
                "Person Kafka source");

        KafkaSource<Auction> auctionKafkaSource =
                KafkaSource.<Auction>builder()
                        .setBootstrapServers(Broker)
                        .setGroupId(auctionKafkaGroup)
                        .setTopics(auctionKafkaTopic)
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(new AuctionSchema()))
                        .setProperty(
                                KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();
        DataStream<Auction> auctions = env.fromSource(
                auctionKafkaSource,
                WatermarkStrategy.<Auction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.dateTime),
                "Auction Kafka source"
        );

        KafkaSource<Bid> bidKafkaSource =
                KafkaSource.<Bid>builder()
                        .setBootstrapServers(Broker)
                        .setGroupId(bidKafkaGroup)
                        .setTopics(bidKafkaTopic)
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(new BidSchema()))
                        .setProperty(
                                KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();
        DataStream<Bid> bids = env.fromSource(
                bidKafkaSource,
                WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((event, timestamp) -> event.dateTime),
                "Bid Kafka source");


        /**
         * Query 7: Highest Bid
         * SELECT Rstream(B.auction, B.price, B.bidder)
         * FROM Bid [RANGE 1 MINUTE SLIDE 1 MINUTE] B
         * WHERE B.price = (SELECT MAX(B1.price)
         *                  FROM BID [RANGE 1 MINUTE SLIDE 1 MINUTE] B1);
         *
         * Query 7 monitors the highest price items currently on auction. Every ten minutes, this query returns the
         * highest bid (and associated itemid) in the most re- cent ten minutes. This query uses a time-based,
         * fixed- window group by.
         */

        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> highestBid = bids
                .keyBy(
                        (Bid bid) -> bid.auction
                )
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .reduce(new ReduceFunction<Bid>() {
                    @Override
                    public Bid reduce(Bid b1, Bid b2) {
                        return b1.price > b2.price ? b1 : b2;
                    }
                }, new ProcessWindowFunction<Bid, Tuple3<Long, Long, Long>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<Bid> elements, Collector<Tuple3<Long, Long, Long>> out) {
                        Bid highestBid = elements.iterator().next();
                        out.collect(new Tuple3<>(highestBid.auction, highestBid.price, highestBid.bidder));
                    }
                });

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        highestBid.transform("Latency Counting Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));
        env.execute("Nexmark Query7");
    }

}
