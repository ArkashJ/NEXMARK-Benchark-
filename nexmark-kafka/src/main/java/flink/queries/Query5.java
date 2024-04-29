package flink.queries;


import flink.sinks.DummyLatencyCountingSink;
import flink.utils.BidSchema;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query5 {

    private static final Logger logger = LoggerFactory.getLogger(Query5.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final float exchangeRate = params.getFloat("exchange-rate", 0.82F);

        final String broker = params.getRequired("broker");
        final String kafkaSrcTopic = params.getRequired("kafka-topic");
        final String kafkaSinkTopic = params.getRequired("sink-topic");
        final String kafkaGroup = params.getRequired("kafka-group");
        // --broker 192.168.1.180:9092 --kafka-topic query5_src ---topic query5_sink --kafka-group 0

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        env.disableOperatorChaining();

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);
        env.getConfig().setAutoWatermarkInterval(5000);

        KafkaSource<Bid> source =
                KafkaSource.<Bid>builder()
                        .setBootstrapServers(broker)
                        .setGroupId(kafkaGroup)
                        .setTopics(kafkaSrcTopic)
                        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new BidSchema()))
                        .setProperty(KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
                        // If each partition has a committed offset, the offset will be consumed
                        // from the committed offset.
                        // Start consuming from scratch when there is no submitted offset
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();

        DataStream<Bid> bids = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source")
                .setParallelism(params.getInt("p-bid-source", 8));

        DataStream<Bid> bidsWithWaterMark =
                bids.assignTimestampsAndWatermarks(new TimestampAssigner()).slotSharingGroup("src");

        // SELECT B1.auction, count(*) AS num
        // FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
        // GROUP BY B1.auction
        DataStream<Tuple2<Long, Long>> windowed = bidsWithWaterMark.keyBy((KeySelector<Bid, Long>) bid -> bid.auction)
                //.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .timeWindow(Time.minutes(60), Time.minutes(1))
                .aggregate(new CountBids())
                .name("Sliding Window");
        //.setParallelism(params.getInt("p-window", 1))
        //.slotSharingGroup("window");


        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        windowed.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-sink", 8));//.slotSharingGroup("sink");

        // execute program
        env.execute("Nexmark Query5");
    }

    private static final class TimestampAssigner implements AssignerWithPeriodicWatermarks<Bid> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Bid element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

    private static final class CountBids implements AggregateFunction<Bid, Long, Tuple2<Long, Long>> {

        private long auction = 0L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Bid value, Long accumulator) {
            auction = value.auction;
            return accumulator + 1;
        }

        @Override
        public Tuple2<Long, Long> getResult(Long accumulator) {
            return new Tuple2<>(auction, accumulator);
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
