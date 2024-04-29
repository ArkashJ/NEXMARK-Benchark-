package flink.queries;

import flink.sinks.DummyLatencyCountingSink;
import flink.utils.AuctionSchema;
import flink.utils.BidSchema;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        final int SECOND = 1000;
        final int MINUTE = 60 * SECOND;
        final int HOUR = 60 * MINUTE;
        final int LATENCY_INTERVAL = 5000;

        // set the kafkas source for bids
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

        // if no rates are provided, use the default rates

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // set the checkpointing interval to 10 minutes, can be adjusted. USE EXACTLY_ONCE!!
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

        // set the source for auctions with NO watermark
        DataStream<Auction> auctions =
                env.fromSource(auctionKafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Auction Kafka source");
        // key the auctions by auction id
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
        // key the bids by auction id
        KeyedStream<Bid, Long> keyedBids =
                bids.keyBy(new KeySelector<Bid, Long>() {
                    @Override
                    public Long getKey(Bid bid) throws Exception {
                        return bid.auction;
                    }
                });

        // First Join using Auction ID
        DataStream<Tuple4<Long, Long, Long, Long>> joined =
                keyedBids.connect(keyedAuctions)
                        .process(new JoinBidsWithAuctions())
                        .name("JoinBidsWithAuctions")
                        .setParallelism(params.getInt("psj", 1));

        // NOTE: Key using Category 
        DataStream<Tuple4<Long, Long, Long, Long>> maxBids =
                joined
                        .keyBy(
                                value -> value.f3
                        )
                        .process(new MaxPriceBid())
                        .name("MaxPriceBid")
                        .setParallelism(params.getInt("psm", 1));

        // Aggregate the results
        KeyedStream<Tuple4<Long, Long, Long, Long>, Object> result = maxBids
                .keyBy(value -> value.f3);

//        DataStream<Tuple3<Long, Long, Double>> result = maxBids.keyBy(value -> value.f2)
//                .process(new AveragePrice())
//                .name("AveragePrice")
//                .setParallelism(params.getInt("psr", 1));
//
        // add a sink to the result stream
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        result.transform("Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("psink", 1));
        env.execute("Nexmark Query4");
    }


    // DO NOT Use CoFlatMap, RichCoFlatMap etc. Since we need watermarks, use CoProcessFunction
    public static class JoinBidsWithAuctions extends CoProcessFunction<Bid, Auction, Tuple4<Long, Long, Long, Long>> {
        // Do not use MapState here, we are already keying the stream by auction id
        // store the auctionEndTime as well for later use
        // bufferedBids is a list of bids that arrived before the auction, use ListState
        private ValueState<Long> auctionEndTime;
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

        // if the auction arrives first, update the auctionEndTime and register a timer
        @Override
        public void processElement2(Auction auction, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            auctionEndTime.update(auction.expires);
            auctionCategory.update(auction.category);
            ctx.timerService().registerEventTimeTimer(auction.expires);
            for (Bid bid : bufferedBids.get()) {
                if (bid.dateTime < auction.expires) {
                    out.collect(new Tuple4<>(bid.auction, bid.price, auction.expires, auction.category));
                }
            }
        }

        // when the auction ends, collect invalid bids
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // when the auction ends, collect invalid bids
            for (Bid bid : bufferedBids.get()) {
                out.collect(new Tuple4<>(bid.auction, bid.price, auctionEndTime.value(), auctionCategory.value()));
            }
            // clear the state in RocksDB, important use clear command
            auctionEndTime.clear();
            bufferedBids.clear();
        }
    }

    public static class MaxPriceBid extends KeyedProcessFunction<Long, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>> {
        private ValueState<Long> maxBid;

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

    public static class AverageAggregate implements AggregateFunction<Tuple4<Long, Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple4<Long, Long, Long, Double>> {
        @Override
        public Tuple3<Long, Long, Long> createAccumulator() {
            return new Tuple3<>(0L, 0L, 0L); // Initialize sum = 0, count = 0, categoryId = 0
        }

        @Override
        public Tuple3<Long, Long, Long> add(Tuple4<Long, Long, Long, Long> value, Tuple3<Long, Long, Long> accumulator) {
            // Update count, sum, and set the category ID (value.f3)
            return new Tuple3<>(accumulator.f0 + 1, accumulator.f1 + value.f1, value.f3);
        }

        @Override
        public Tuple4<Long, Long, Long, Double> getResult(Tuple3<Long, Long, Long> accumulator) {
            // Return categoryId, count, sum, and average
            double average = (accumulator.f0 == 0) ? 0 : (double) accumulator.f1 / accumulator.f0;
            return new Tuple4<>(accumulator.f2, accumulator.f0, accumulator.f1, average); // categoryId, count, sum, average
        }

        @Override
        public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> a, Tuple3<Long, Long, Long> b) {
            // Ensure the category ID is carried over (assuming both 'a' and 'b' are from the same category)
            return new Tuple3<>(a.f0 + b.f0, a.f1 + b.f1, a.f2);
        }
    }

}

