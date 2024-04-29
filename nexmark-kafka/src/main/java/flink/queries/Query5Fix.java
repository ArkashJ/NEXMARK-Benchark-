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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description This query selects the item with the most bids in the past one hour time period; the “hottest” item. The results are output every minute.
 * This query uses a time-based, sliding window group by operation
 * @SQL SELECT Rstream(auction)
 * FROM (SELECT B1.auction, count(*) AS num
 * FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
 * GROUP BY B1.auction)
 * WHERE num >= ALL (SELECT count(*)
 * FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
 * GROUP BY B2.auction);
 * @@author arkashjain
 */
public class Query5Fix {
    private static final Logger logger = LoggerFactory.getLogger(Query5Fix.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final String broker = params.getRequired("broker");
        final String kafkaSrcTopic = params.getRequired("kafka-topic");
        final String kafkaSinkTopic = params.getRequired("sink-topic");
        final String kafkaGroup = params.getRequired("kafka-group");
        // --broker
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<Bid> source =
                KafkaSource.<Bid>builder()
                        .setBootstrapServers(broker)
                        .setGroupId(kafkaGroup)
                        .setTopics(kafkaSrcTopic)
                        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new BidSchema()))
                        .setProperty(KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();
        DataStream<Bid> bids = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        KeyedStream<Bid, Long> keyedBids = bids.keyBy(new KeySelector<Bid, Long>() {
            @Override
            public Long getKey(Bid value) throws Exception {
                return value.auction;
            }
        });

        SingleOutputStreamOperator<String> auctionCount = keyedBids.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(1)))
                .aggregate(
                        // Count the number of bids per auction
                        new BidCountAggregate()
                        // Output the auction with the highest number of bids
                        , new AuctionWindowFunction());
        // Output the auction with the highest number of bids

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        auctionCount.transform("Latency Counting Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger)).name("Latency Counting Sink");

        auctionCount.print();
        env.execute("Nexmark Query5Fix");
    }

    private static class BidCountAggregate implements AggregateFunction<Bid, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        /**
         * Create an accumulator for the bid count
         *
         * @return
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        /**
         * Add a bid to the accumulator
         *
         * @param value
         * @param accumulator
         * @return
         */
        @Override
        public Tuple2<Long, Long> add(Bid value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<>(value.auction, accumulator.f1 + 1);
        }

        /**
         * @param accumulator
         * @return
         * @Description: get the result of the accumulator
         */
        @Override
        public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
            return accumulator;
        }

        /**
         * Merge two accumulators
         *
         * @param a
         * @param b
         * @return
         */
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<>(a.f0, a.f1 + b.f1);
        }
    }

    /**
     * @Description: process the highest bid
     */
    private static class AuctionWindowFunction extends ProcessWindowFunction<Tuple2<Long, Long>, String, Long, TimeWindow> {
        /**
         * @param key      The key for which this window is evaluated.
         * @param context  The context in which the window is being evaluated.
         * @param elements The elements in the window being evaluated.
         * @param out      A collector for emitting elements.
         * @Description: iterate over the elements and get the highest bid
         */
        @Override
        public void process(Long key, Context context, Iterable<Tuple2<Long, Long>> elements, Collector<String> out) {
            Tuple2<Long, Long> maxBid = new Tuple2<>(key, Long.MIN_VALUE);
            for (Tuple2<Long, Long> element : elements) {
                if (element.f1 > maxBid.f1) {
                    // get the highest bid
                    maxBid = element;
                }
            }
            // Only emit if this is the max for the current window
            boolean isMax = true;
            for (Tuple2<Long, Long> element : elements) {
                if (element.f1 > maxBid.f1) {
                    isMax = false;
                    break;
                }
            }
            if (isMax) {
                out.collect("Auction " + maxBid.f0 + " had the highest number of bids: " + maxBid.f1 + " in the window ending " + context.window().getEnd());
            }
        }
    }
}

