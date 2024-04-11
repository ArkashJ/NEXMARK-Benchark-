package flink.queries;


import flink.sinks.DummyLatencyCountingSink;
import flink.sources.AuctionSourceFunction;
import flink.sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import flink.utils.AuctionSchema;
import flink.utils.PersonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

public class Query8 {

    private static final Logger logger = LoggerFactory.getLogger(Query8.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final String Broker = params.getRequired("broker");
        final String auctionKafkaTopic = params.getRequired("auction-kafka-topic");
        final String auctionKafkaGroup = params.getRequired("auction-kafka-group");
        final String personKafkaTopic = params.getRequired("person-kafka-topic");
        final String personKafkaGroup = params.getRequired("person-kafka-group");
        // --broker 192.168.1.180:9092 --auction-kafka-topic query8_a --auction-kafka-group 0 --person-kafka-topic query8_p --person-kafka-group 0

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setCheckpointTimeout(30000);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(100000);

        env.setParallelism(4);      //set parallelism for join


        KafkaSource<Person> personKafkaSource =
                KafkaSource.<Person>builder()
                        .setBootstrapServers(Broker)
                        .setGroupId(personKafkaGroup)
                        .setTopics(personKafkaTopic)
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(new PersonSchema()))
                        .setProperty(
                                KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
                        // If each partition has a committed offset, the offset will be consumed
                        // from the committed offset.
                        // Start consuming from scratch when there is no submitted offset
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();
        DataStream<Person> persons =
                env.fromSource(personKafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                "Person Kafka source")
                        .setParallelism(1)
                        .slotSharingGroup("person-src");
        DataStream<Person> personsWithWaterMark =
                persons.assignTimestampsAndWatermarks(new PersonTimestampAssigner())
                        .setParallelism(1)
                        .slotSharingGroup("person-timestamp");

        KafkaSource<Auction> auctionKafkaSource =
                KafkaSource.<Auction>builder()
                        .setBootstrapServers(Broker)
                        .setGroupId(auctionKafkaGroup)
                        .setTopics(auctionKafkaTopic)
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(new AuctionSchema()))
                        .setProperty(
                                KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
                        // If each partition has a committed offset, the offset will be consumed
                        // from the committed offset.
                        // Start consuming from scratch when there is no submitted offset
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();
        DataStream<Auction> auctions =
                env.fromSource(auctionKafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                "Auction Kafka source")
                        .setParallelism(1)
                        .slotSharingGroup("auc-src");
        DataStream<Auction> auctionsWithWaterMark =
                auctions.assignTimestampsAndWatermarks(new AuctionTimestampAssigner())
                        .setParallelism(1)
                        .slotSharingGroup("auc-timestamp");

        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        DataStream<Tuple3<Long, String, Long>> joined =
                personsWithWaterMark.join(auctionsWithWaterMark)
                .where(new KeySelector<Person, Long>() {
                    @Override
                    public Long getKey(Person p) {
                        return p.id;
                    }
                }).equalTo(new KeySelector<Auction, Long>() {
                            @Override
                            public Long getKey(Auction a) {
                                return a.seller;
                            }
                        })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new FlatJoinFunction<Person, Auction, Tuple3<Long, String, Long>>() {
                    @Override
                    public void join(Person p, Auction a, Collector<Tuple3<Long, String, Long>> out) {
                        out.collect(new Tuple3<>(p.id, p.name, a.reserve));
                    }
                });


        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        joined.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-window", 1));//.slotSharingGroup("sink");

        // execute program
        env.execute("Nexmark Query8");
    }

    private static final class PersonTimestampAssigner implements AssignerWithPeriodicWatermarks<Person> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Person element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

    private static final class AuctionTimestampAssigner implements AssignerWithPeriodicWatermarks<Auction> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Auction element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

}