
package flink.queries;
import org.apache.flink.api.common.state.*;
import flink.sources.BidSourceFunction;
import flink.sinks.DummyLatencyCountingSink;
import flink.sources.AuctionSourceFunction;
import flink.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import flink.utils.AuctionSchema;
import flink.utils.BidSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import java.util.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.time.Time;
/**
 *
 * Query 6: Average Selling Price by Seller
 * SELECT Istream(AVG(Q.final), Q.seller)
 * FROM (SELECT Rstream(MAX(B.price) AS final, A.seller)
 *       FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
 *       WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
 *       GROUP BY A.id, A.seller) [PARTITION BY A.seller ROWS 10] Q
 * GROUP BY Q.seller;
 */

public class Query6{
    private static final Logger logger = LoggerFactory.getLogger(Query6.class);
    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);
        // kafka parameters
        final String Broker = params.getRequired("broker");
        final String auctionKafkaTopic = params.getRequired("auction-kafka-topic");
        final String auctionKafkaGroup = params.getRequired("auction-kafka-group");
        final String bidKafkaTopic = params.getRequired("bid-kafka-topic");
        final String bidKafkaGroup = params.getRequired("bid-kafka-group");
        // Time parameters
        final int SECOND= 1000;
        final int MINUTE= 60 * SECOND;
        final int HOUR = 60 *MINUTE;
        final int LATENCY_INTERVAL= 5000;
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
                .keyBy(new KeySelector<Bid, Long>(){
                    @Override
                    public Long getKey(Bid bid) throws Exception {
                        return bid.auction;
                    }
                });

        DataStream<Tuple4<Long, Long, Long, Long>> joinedStream = keyedBids.connect(keyedAuctions)
                .process(new JoinBidsWithAuctions())
                .name("JoinBidsWithAuctions");

        KeyedStream<Tuple4<Long, Long, Long, Long>, Long> keyedJoinedStream = joinedStream
                .keyBy(new KeySelector<Tuple4<Long, Long, Long, Long>, Long>(){
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
                .keyBy(new KeySelector<Tuple4<Long, Long, Long, Long>, Long>(){
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
                .setParallelism(params.getInt("psink", 1))
                .slotSharingGroup("sink");
        env.execute("Flink Query6");
    }

    private static class JoinBidsWithAuctions extends CoProcessFunction<Bid, Auction, Tuple4<Long, Long, Long, Long>> {
        private ValueState<Long> auctionEndTime;
        // bufferedBids is a list of bids that arrived before the auction, use ListState
        private ListState<Bid> bufferedBids;
        private ValueState<Long> auctionSeller;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> endTimeDescriptor = new ValueStateDescriptor<>("auctionEndTime", LongSerializer.INSTANCE);
            auctionEndTime = getRuntimeContext().getState(endTimeDescriptor);

            ValueStateDescriptor<Long> sellerDescriptor = new ValueStateDescriptor<>("auctionSeller", LongSerializer.INSTANCE);
            auctionSeller = getRuntimeContext().getState(sellerDescriptor);
            // Using a ListState  store a list of bid, furthermore, passing Bid.class leads to automatic serialization and deserialization
            ListStateDescriptor<Bid> bufferedBidsDescriptor = new ListStateDescriptor<>(
                    "bufferedBids",
                    Bid.class);
            bufferedBids = getRuntimeContext().getListState(bufferedBidsDescriptor);
        }

        @Override
        public void processElement1(Bid bid, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            Long endTime = auctionEndTime.value();
            Long seller = auctionSeller.value();
            if (endTime != null && bid.dateTime < endTime) {
                // if the bid arrived before the auction ended, emit it downstream
                out.collect(new Tuple4<>(bid.auction, bid.price, endTime, seller));
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
                    out.collect(new Tuple4<>(bid.auction, bid.price, auction.expires, auction.seller));
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            // when the auction ends, collect invalid bids
            for (Bid bid : bufferedBids.get()) {
                out.collect(new Tuple4<>(bid.auction, bid.price, auctionEndTime.value(), auctionSeller.value()));
            }
            auctionEndTime.clear();
            bufferedBids.clear();
        }
    }


    private static class CalculateMaxBid extends KeyedProcessFunction<Long, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>>{
        // State to hold the current maximum bid for each auction
        private ValueState<Long> maxBidState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize the state descriptor
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "maxBid", // the state name
                    TypeInformation.of(new TypeHint<Long>() {}), // type information
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
}
