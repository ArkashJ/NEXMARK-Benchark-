
package flink.queries;
import flink.sources.BidSourceFunction;
import flink.sinks.DummyLatencyCountingSink;
import flink.sources.AuctionSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import java.util.*;
import flink.utils.AuctionSchema;
import flink.utils.BidSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.flink.api.common.typeinfo.*;
import java.util.*;
public class Query4 {
    // Query4Stateful logger
    private static final Logger logger = LoggerFactory.getLogger(Query4.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String Broker = params.getRequired("broker");
        final String auctionKafkaTopic = params.getRequired("auction-kafka-topic");
        final String auctionKafkaGroup = params.getRequired("auction-kafka-group");

        final String bidKafkaTopic = params.getRequired("bid-kafka-topic");
        final String bidKafkaGroup = params.getRequired("bid-kafka-group");

        final int SECOND= 1000;
        final int MINUTE= 60 * SECOND;
        final int HOUR = 60 *MINUTE;
        final int LATENCY_INTERVAL= 5000;

//        KafkaSource<Auction> auctionKafkaSource =
//                KafkaSource.<Auction>builder()
//                        .setBootstrapServers(Broker)
//                        .setGroupId(auctionKafkaGroup)
//                        .setTopics(auctionKafkaTopic)
//                        .setDeserializer(
//                                KafkaRecordDeserializationSchema.valueOnly(new AuctionSchema()))
//                        .setProperty(
//                                KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
//                        .setStartingOffsets(
//                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
//                        .build();

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
        List<List<Integer>> auctionSrcRates = new ArrayList<>();
        List<List<Integer>> bidSrcRates = new ArrayList<>();

        auctionSrcRates.add(Arrays.asList(50000, 300000, 10000, 300000, 1000, 600000, 200, 600000));
        bidSrcRates.add(Arrays.asList(60000, 200000, 10000, 200000, 1000, 300000, 200, 300000));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        int checkpointinginterval = 10 * MINUTE;
        env.enableCheckpointing(checkpointinginterval, CheckpointingMode.EXACTLY_ONCE);

        env.getConfig().setLatencyTrackingInterval(LATENCY_INTERVAL);
        env.getConfig().setGlobalJobParameters(params);

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
                        // Start consuming from earlist when there is no submitted offset
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();

        DataStream<Auction> auctions =
                env.fromSource(auctionKafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                "Auction Kafka source");

        KeyedStream<Auction, Long> keyedAuctions =
                auctions.keyBy(new KeySelector<Auction, Long>() {
                    @Override
                    public Long getKey(Auction auction) throws Exception {
                        return auction.id;
                    }
                });

        DataStream<Bid> bids =
                env.fromSource(bidKafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Bid Kafka source");

        KeyedStream<Bid, Long> keyedBids =
                bids.keyBy(new KeySelector<Bid, Long>() {
                    @Override
                    public Long getKey(Bid bid) throws Exception {
                        return bid.auction;
                    }
                });

        DataStream<Tuple4<Long, Long, Long, Long>> joined =
                keyedBids.connect(keyedAuctions)
                        .process(new JoinBidsWithAuctions())
                        .name("JoinBidsWithAuctions")
                        .setParallelism(params.getInt("psj", 1));

        DataStream<Tuple4<Long, Long, Long, Long>> maxBids =
                joined
                        .keyBy(
                                value -> value.f3
                        )
                        .process(new MaxPriceBid())
                        .name("MaxPriceBid")
                        .setParallelism(params.getInt("psm", 1));

        DataStream<Tuple3<Long, Long, Double>> result = maxBids.keyBy(value -> value.f2)
                .process(new AveragePrice())
                .name("AveragePrice")
                .setParallelism(params.getInt("psr", 1));

        // add a sink to the result stream
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        result.transform("Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("psink", 1))
                .slotSharingGroup("sink");
        env.execute("Nexmark Query4");
    }

    private static class JoinBidsWithAuctions extends CoProcessFunction<Bid, Auction, Tuple4<Long, Long, Long, Long>> {
        private ValueState<Long> auctionEndTime;
        // bufferedBids is a list of bids that arrived before the auction, use ListState
        private ListState<Bid> bufferedBids;
        private ValueState<Long> auctionCategory;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> endTimeDescriptor = new ValueStateDescriptor<>("auctionEndTime", LongSerializer.INSTANCE);
            auctionEndTime = getRuntimeContext().getState(endTimeDescriptor);

            ValueStateDescriptor<Long> categoryDescriptor = new ValueStateDescriptor<>("auctionCategory", LongSerializer.INSTANCE);
            auctionCategory = getRuntimeContext().getState(categoryDescriptor);
            // Using a ListState  store a list of bid, furthermore, passing Bid.class leads to automatic serialization and deserialization
            ListStateDescriptor<Bid> bufferedBidsDescriptor = new ListStateDescriptor<>(
                    "bufferedBids",
                    Bid.class);
            bufferedBids = getRuntimeContext().getListState(bufferedBidsDescriptor);
        }

        @Override
        public void processElement1(Bid bid, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            Long endTime = auctionEndTime.value();
            Long category = auctionCategory.value();
            if (endTime != null && bid.dateTime < endTime) {
                // if the bid arrived before the auction ended, emit it downstream
                out.collect(new Tuple4<>(bid.auction, bid.price, endTime, category));
            } else {
                // otherwise, buffer the bid
                bufferedBids.add(bid);
            }
        }

        @Override
        public void processElement2(Auction auction, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            auctionEndTime.update(auction.expires);
            ctx.timerService().registerEventTimeTimer(auction.expires);
            for (Bid bid : bufferedBids.get()) {
                if (bid.dateTime < auction.expires) {
                    out.collect(new Tuple4<>(bid.auction, bid.price, auction.expires, auction.category));
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // when the auction ends, collect invalid bids
            for (Bid bid : bufferedBids.get()) {
                out.collect(new Tuple4<>(bid.auction, bid.price, auctionEndTime.value(), auctionCategory.value()));
            }
            auctionEndTime.clear();
            bufferedBids.clear();
        }
    }

    private static class MaxPriceBid extends KeyedProcessFunction<Long, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>> {
        private transient ValueState<Long> maxBid;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "maxBid",
                    Long.class);
            maxBid = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple4<Long, Long, Long, Long> value, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            Long currentMaxBid = maxBid.value();
            if (currentMaxBid == null || value.f1 > currentMaxBid) {
                maxBid.update(value.f1);
                // Emit new max bid for this auction
                out.collect(new Tuple4<>(value.f0, value.f1, value.f2, value.f3));
                // Auction ID, Max Bid, Auction End Time, Category
            }
        }
    }

    private static class AveragePrice extends KeyedProcessFunction<Long, Tuple4<Long, Long, Long, Long>, Tuple3<Long, Long, Double>> {
        // track the sum of maxbid prices and count of auctions per category
        private transient ValueState<Tuple2<Long, Long>> categoryState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                    "categoryState",
                    Types.TUPLE(Types.LONG, Types.LONG));
            categoryState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple4<Long, Long, Long, Long> value, Context ctx, Collector<Tuple3<Long, Long, Double>> out) throws Exception {
            // Tuple structure: <AuctionID, MaxBid, AuctionEndTime, CategoryID>
            Long categoryID = value.f3;
            Tuple2<Long, Long> currentState = categoryState.value();
            if (currentState == null) {
                currentState = Tuple2.of(0L, 0L);
            }
            currentState.f0 += value.f1; // Update sum with the max bid price
            currentState.f1 += 1; // Increment count
            categoryState.update(currentState);

            // Calculate the average price for this category
            double avgPrice = (double) currentState.f0 / currentState.f1;
            out.collect(new Tuple3<>(categoryID, currentState.f1, avgPrice));
        }
    }

//    @Test
//    public void testQuery4() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);
//        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
//        env.getConfig().setLatencyTrackingInterval(1000);
//        env.setParallelism(1);
//
//        List<Auction> auctions = new ArrayList<>();
//        auctions.add(new Auction(1L, 1L, 100L, 1000L, 1000));
//        auctions.add(new Auction(2L, 2L, 200L, 2000L, 2000));
//        auctions.add(new Auction(3L, 3L, 300L, 3000L, 3000));
//
//        List<Bid> bids = new ArrayList<>();
//        bids.add(new Bid(1L, 1L, 100L, 1000L));
//        bids.add(new Bid(2L, 2L, 200L, 2000L));
//        bids.add(new Bid(3L, 3L, 300L, 3000L));
//        bids.add(new Bid(1L, 4L, 400L, 4000L));
//        bids.add(new Bid(2L, 5L, 500L, 5000L));
//        bids.add(new Bid(3L, 6L, 600L, 6000L));
//        bids.add(new Bid(1L, 7L, 700L, 7000L));
//
//        DataStream<Auction> auctionStream = env.fromCollection(auctions);
//        DataStream<Bid> bidStream = env.fromCollection(bids);
//
//        DataStream<Tuple4<Long, Long, Long, Long>> joined = bidStream.connect(auctionStream)
//                .process(new JoinBidsWithAuctions());
//
//        DataStream<Tuple4<Long, Long, Long, Long>> maxBids = joined
//                .keyBy(value -> value.f3)
//                .process(new MaxPriceBid());
//
//        // Calculate the average price for each category
//        assert maxBids != null;
//
//        assertEquals(maxBids.keyBy(value -> value.f2)
//                .process(new AveragePrice())
//                .collect()
//                .toString(), "[3, 2, 6.0], [2, 2, 5.0], [1, 3, 7.0]");
//
//        DataStream<Tuple3<Long, Long, Double>> result = maxBids.keyBy(value -> value.f2)
//                .process(new AveragePrice());
//
//        List<Tuple3<Long, Long, Double>> expected = new ArrayList<>();
//        expected.add(new Tuple3<>(1000L, 3L, 7.0));
//        expected.add(new Tuple3<>(2000L, 2L, 5.0));
//        expected.add(new Tuple3<>(3000L, 2L, 6.0));
//
//        List<Tuple3<Long, Long, Double>> actual = new ArrayList<>();
//        result.collect().forEach(actual::add);
//
//        assertEquals(expected, actual);
//
//
//        env.execute("Query4 Test");
//    }
}
