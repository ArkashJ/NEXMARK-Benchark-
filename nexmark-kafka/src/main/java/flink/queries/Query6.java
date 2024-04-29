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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Query 6: Average Selling Price by Seller
 * SELECT Istream(AVG(Q.final), Q.seller)
 * FROM (SELECT Rstream(MAX(B.price) AS final, A.seller)
 * FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
 * WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
 * GROUP BY A.id, A.seller) [PARTITION BY A.seller ROWS 10] Q
 * GROUP BY Q.seller;
 */

/**
 * @@author arkashjain
 * @@since 2024
 */
public class Query6 {
    private static final Logger logger = LoggerFactory.getLogger(Query6.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // kafka parameters
        final String Broker = params.getRequired("broker");
        final String auctionKafkaTopic = params.getRequired("auction-kafka-topic");
        final String auctionKafkaGroup = params.getRequired("auction-kafka-group");
        final String bidKafkaTopic = params.getRequired("bid-kafka-topic");
        final String bidKafkaGroup = params.getRequired("bid-kafka-group");
        // Time parameters
        final int SECOND = 1000;
        final int MINUTE = 60 * SECOND;
        final int HOUR = 60 * MINUTE;
        final int LATENCY_INTERVAL = 5000;

        // Flink environment setup
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the EventTime characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        // Checkpointing configuration
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
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();

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
        // fixed rate from Kafka topic
        DataStream<Auction> auctions = env.fromSource(auctionKafkaSource, WatermarkStrategy.noWatermarks(), "Auction Source");
        DataStream<Bid> bids = env.fromSource(bidKafkaSource, WatermarkStrategy.noWatermarks(), "Bid Source");

        // Key auctions and bids by their respective IDs
        KeyedStream<Auction, Long> keyedAuctions = auctions
                .keyBy(
                        new KeySelector<Auction, Long>() {
                            @Override
                            public Long getKey(Auction auction) throws Exception {
                                return auction.id;
                            }
                        }
                );

        KeyedStream<Bid, Long> keyedBids = bids
                .keyBy(new KeySelector<Bid, Long>() {
                    @Override
                    public Long getKey(Bid bid) throws Exception {
                        return bid.auction;
                    }
                });

        // Emit a joint stream of Auctions and Bids keyed by the auction ID
        DataStream<Tuple4<Long, Long, Long, Long>> joinedStream = keyedBids.connect(keyedAuctions)
                .process(new JoinBidsWithAuctions())
                .name("JoinBidsWithAuctions");

        KeyedStream<Tuple4<Long, Long, Long, Long>, Long> keyedJoinedStream = joinedStream
                .keyBy(new KeySelector<Tuple4<Long, Long, Long, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple4<Long, Long, Long, Long> value) throws Exception {
                        return value.f3;
                    }
                });

        // implement partitioning as well
        DataStream<Tuple4<Long, Long, Long, Long>> maxBidStream = keyedJoinedStream
                .process(new CalculateMaxBid())
                .name("CalculateMaxBid");

        DataStream<Tuple2<Long, Double>> avgPriceStream = maxBidStream
                .keyBy(new KeySelector<Tuple4<Long, Long, Long, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple4<Long, Long, Long, Long> value) throws Exception {
                        return value.f3;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Tuple4<Long, Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple2<Long, Double>>() {
                    // Tuple structure: <AuctionID, MaxBid, AuctionEndTime, SellerID>
                    @Override
                    public Tuple3<Long, Long, Long> createAccumulator() {
                        return Tuple3.of(0L, 0L, 0L); // Sum of max bids, count, sellerID
                    }
                    // For every bid calculate the sum of max bids and count

                    @Override
                    public Tuple3<Long, Long, Long> add(Tuple4<Long, Long, Long, Long> value, Tuple3<Long, Long, Long> accumulator) {
                        return Tuple3.of(accumulator.f0 + value.f1, accumulator.f1 + 1, value.f3);
                    }

                    // Calculate the average price
                    @Override
                    public Tuple2<Long, Double> getResult(Tuple3<Long, Long, Long> accumulator) {
                        // Calculate average
                        return Tuple2.of(accumulator.f2, accumulator.f0.doubleValue() / accumulator.f1);
                    }

                    // Merge the accumulators
                    @Override
                    public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> a, Tuple3<Long, Long, Long> b) {
                        return Tuple3.of(a.f0 + b.f0, a.f1 + b.f1, a.f2);
                    }
                })
                .name("AggregateFunction");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        avgPriceStream.transform("Sink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("psink", 1));

        env.execute("Flink Query6");
    }


    /**
     * @Description: This function joins the Auction and Bid streams and emits the bids that arrived before the auction ended.
     * The function also buffers the bids that arrived after the auction ended.
     * @@implNote The function uses a CoProcessFunction to join the Auction and Bid streams.
     * @@hidden
     */
    public static class JoinBidsWithAuctions extends CoProcessFunction<Bid, Auction, Tuple4<Long, Long, Long, Long>> {
        private ListState<Bid> bufferedBids;
        private ValueState<Auction> auctionState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Auction> auctionDescriptor = new ValueStateDescriptor<>(
                    "auction-state", // state name
                    Auction.class // type information
            );
            auctionState = getRuntimeContext().getState(auctionDescriptor);

            ListStateDescriptor<Bid> bufferedBidsDescriptor = new ListStateDescriptor<>(
                    "buffered-bids", // state name
                    Bid.class // type information
            );
            bufferedBids = getRuntimeContext().getListState(bufferedBidsDescriptor);
        }

        @Override
        public void processElement1(Bid bid, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // Retrieve the current auction state
            Auction auction = auctionState.value();
            // UNCOMMENT TO DEBUG
//            System.out.println(""
//                    + "Bid: " + bid
//                    + "Auction: " + auction + "\n\n"
//                    + "---------------------------------"
//            );
            long currentTime = ctx.timerService().currentWatermark();
            // if the auction is not null and it has not expired
            if (auction != null && auction.expires > bid.dateTime && auction.expires > currentTime) {
                // Emit the bid if it arrived before the auction ended
                out.collect(new Tuple4<>(auction.id, bid.price, auction.expires, auction.seller));
            } else {
                // Buffer the bid if it arrived after the auction ended
                bufferedBids.add(bid);
            }
        }

        @Override
        public void processElement2(Auction auction, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // Update the auction state
            auctionState.update(auction);
            // Retrieve the buffered bids
            for (Bid bid : bufferedBids.get()) {
                // Emit the buffered bids
                out.collect(new Tuple4<>(auction.id, bid.price, auction.expires, auction.seller));
            }
            // Clear the buffered bids
            bufferedBids.clear();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // Retrieve the current auction state
            Auction auction = auctionState.value();
            // Retrieve the buffered bids
            for (Bid bid : bufferedBids.get()) {
                // Emit the buffered bids
                out.collect(new Tuple4<>(auction.id, bid.price, auction.expires, auction.seller));
            }
            // Clear the buffered bids
            bufferedBids.clear();
            auctionState.clear();
        }
    }

    /**
     * @Description: This function calculates the maximum bid for each auction.
     * @@implNote The function uses a KeyedProcessFunction to calculate the maximum bid for each auction.
     * @@hidden
     */
    public static class CalculateMaxBid extends KeyedProcessFunction<Long, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>> {
        // State to hold the current maximum bid for each auction
        private ValueState<Long> maxBidState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize the state descriptor
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "maxBid", // the state name
                    TypeInformation.of(new TypeHint<Long>() {
                    }), // type information
                    Long.MIN_VALUE); // default value
            maxBidState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple4<Long, Long, Long, Long> value, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // Retrieve the current maximum bid
            Long currentMaxBid = maxBidState.value();
            if (value.f1 > currentMaxBid) {
                // Update the state with the new maximum bid
                maxBidState.update(value.f1);
                // Emit the new maximum bid along with the auction ID and bid time
                out.collect(new Tuple4<>(value.f0, value.f1, value.f2, value.f3));
                // AuctionID, MaxBid, AuctionEndTime, Seller
            }
        }
    }

    public static class AverageAggregate implements AggregateFunction<Tuple4<Long, Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple2<Long, Double>> {
        @Override
        public Tuple3<Long, Long, Long> createAccumulator() {
            System.out.println("Entered createAccumulator**************");
            return Tuple3.of(0L, 0L, 0L); // Sum of max bids, count, sellerID
        }

        // For every bid calculate the sum of max bids and count
        @Override
        public Tuple3<Long, Long, Long> add(Tuple4<Long, Long, Long, Long> value, Tuple3<Long, Long, Long> accumulator) {
            System.out.println("Entered add, values are: " + value + "**************");
            return Tuple3.of(accumulator.f0 + value.f1, accumulator.f1 + 1, value.f3);
        }

        // Calculate the average price
        @Override
        public Tuple2<Long, Double> getResult(Tuple3<Long, Long, Long> accumulator) {
            System.out.println("Entered getResult, values are: " + accumulator + "**************");
            // Calculate average
            return Tuple2.of(accumulator.f2, accumulator.f0.doubleValue() / accumulator.f1);
        }

        // Merge the accumulators
        @Override
        public Tuple3<Long, Long, Long> merge(Tuple3<Long, Long, Long> a, Tuple3<Long, Long, Long> b) {
            return Tuple3.of(a.f0 + b.f0, a.f1 + b.f1, a.f2);
        }
    }
}
