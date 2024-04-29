package flink.test;

import flink.queries.Query4;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
// https://junit.org/junit4/javadoc/4.12/org/junit/Test.html

/**
 * Test for Query4.
 *
 * @@author arkashjain
 */
public class testQuery4 {

    @Test
    public void testQuery4() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(new String[]{});

        List<Auction> auctionList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            auctionList.add(new Auction(i, "item" + i, "description" + i, random.nextInt(90) + 10,
                    random.nextInt(90) + 10, System.currentTimeMillis() + 1000, System.currentTimeMillis() + 5000, random.nextInt(10) + 1,
                    random.nextInt(10), "extra"));
        }
        DataStream<Auction> auctionStream = env.fromCollection(auctionList).assignTimestampsAndWatermarks(WatermarkStrategy.<Auction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((auction, timestamp) -> auction.dateTime));
        KeyedStream<Auction, Long> keyedAuctions =
                auctionStream.keyBy(new KeySelector<Auction, Long>() {
                    @Override
                    public Long getKey(Auction auction) throws Exception {
                        return auction.id;
                    }
                });

        // Make a bids stream
        List<Bid> bidList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            bidList.add(new Bid(i, random.nextInt(10) + 1, random.nextInt(90) + 10, System.currentTimeMillis(), "extra"));
        }
        DataStream<Bid> bidStream = env.fromCollection(bidList).assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((bid, timestamp) -> bid.dateTime));
        KeyedStream<Bid, Long> keyedBids =
                bidStream.keyBy(new KeySelector<Bid, Long>() {
                    @Override
                    public Long getKey(Bid bid) throws Exception {
                        return bid.auction;
                    }
                });

        DataStream<Tuple4<Long, Long, Long, Long>> result = keyedBids.connect(keyedAuctions)
                .process(new Query4.JoinBidsWithAuctions());


        DataStream<Tuple4<Long, Long, Long, Long>> maxBids =
                result
                        .keyBy(
                                value -> value.f3
                        )
                        .process(new Query4.MaxPriceBid())
                        .name("MaxPriceBid");


        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> averages = maxBids.keyBy(value -> value.f3)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new Query4.AverageAggregate());

        averages.addSink(new SinkFunction<Tuple4<Long, Long, Long, Double>>() {
            @Override
            public void invoke(Tuple4<Long, Long, Long, Double> value, Context context) {
                System.out.println("*************" + value + "*************");
            }
        });

        env.execute();
    }

    @Test
    public void testAveragePriceByCategory() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Ensuring determinism for the test
        // Auction.java has a constructor that takes:
        /**
         * public Auction(long id, String itemName, String description, long initialBid, long reserve,
         *       long dateTime, long expires, long seller, long category, String extra)
         */
        List<Auction> auctions = new ArrayList<>();
        auctions.add(new Auction(1, "Item1", "Description1", 10, 100, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 1, 1, ""));
        auctions.add(new Auction(2, "Item2", "Description2", 20, 200, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 2, 2, ""));
        auctions.add(new Auction(3, "Item3", "Description3", 30, 300, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 3, 1, ""));
        auctions.add(new Auction(4, "Item4", "Description4", 40, 400, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 4, 2, ""));
        auctions.add(new Auction(5, "Item5", "Description5", 50, 500, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 5, 1, ""));
        // Create a DataStream from the list of Auctions
        DataStream<Auction> auctionStream = env.fromCollection(auctions)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Auction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((auction, timestamp) -> auction.dateTime));

        /**
         * public Bid(long auction, long bidder, long price, long dateTime, String extra)
         */
        List<Bid> bids = new ArrayList<>();
        bids.add(new Bid(1, 1, 10, System.currentTimeMillis(), ""));
        bids.add(new Bid(1, 2, 20, System.currentTimeMillis(), ""));
        bids.add(new Bid(1, 3, 30, System.currentTimeMillis(), ""));
        bids.add(new Bid(1, 4, 40, System.currentTimeMillis() + 1000, ""));
        bids.add(new Bid(1, 5, 50, System.currentTimeMillis() + 1500, ""));
        bids.add(new Bid(1, 6, 60, System.currentTimeMillis() + 750, ""));
        bids.add(new Bid(2, 2, 20, System.currentTimeMillis() + 1000, ""));
        bids.add(new Bid(2, 3, 30, System.currentTimeMillis() + 750, ""));
        bids.add(new Bid(2, 4, 40, System.currentTimeMillis() + 1500, ""));

        // add watermark to the stream
        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((bid, timestamp) -> bid.dateTime));

        // Make a keyed stream
        KeyedStream<Auction, Long> keyedAuctions = auctionStream.keyBy(new KeySelector<Auction, Long>() {
            @Override
            public Long getKey(Auction auction) throws Exception {
                return auction.category;
            }
        });

        KeyedStream<Bid, Long> keyedBids = bidDataStream.keyBy(new KeySelector<Bid, Long>() {
            @Override
            public Long getKey(Bid bid) throws Exception {
                return bid.auction;
            }
        });

        // Join the two streams
        DataStream<Tuple4<Long, Long, Long, Long>> joinedStream = keyedBids.connect(keyedAuctions)
                .process(new Query4.JoinBidsWithAuctions());

        DataStream<Tuple4<Long, Long, Long, Long>> maxBids = joinedStream
                .keyBy(value -> value.f3)
                .process(new Query4.MaxPriceBid());


        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> averages = maxBids.keyBy(value -> value.f3)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new Query4.AverageAggregate());

        List<Tuple4<Long, Long, Long, Double>> results = averages.executeAndCollect(2);
        System.out.println("*************" + results + "*************");
        // expected results
        List<Tuple4<Long, Long, Long, Double>> expectedResults = Arrays.asList(
                new Tuple4<>(1L, 6L, 210L, 35.0),
                new Tuple4<>(2L, 3L, 90L, 30.0)
        );
        assertEquals(expectedResults.size(), results.size());
        assertEquals(expectedResults, results);
    }

    //    @Test
    public void testComplexWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Ensuring determinism for the test
        long currentTime = System.currentTimeMillis();
        // Auctions data setup with overlapping windows
        List<Auction> auctions = new ArrayList<>();
        auctions.add(new Auction(1, "Laptop", "Electronics", 500, 1000, currentTime, currentTime + 20000, 1, 1, "extra1"));
        auctions.add(new Auction(2, "Camera", "Photography", 300, 600, currentTime + 5000, currentTime + 25000, 2, 2, "extra2"));
        auctions.add(new Auction(3, "Smartphone", "Electronics", 200, 400, currentTime + 10000, currentTime + 30000, 1, 1, "extra3"));
        // Create a DataStream from the list of Auctions
        DataStream<Auction> auctionStream = env.fromCollection(auctions)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Auction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((auction, timestamp) -> auction.dateTime));
        // Bids data setup
        List<Bid> bids = new ArrayList<>();
        bids.add(new Bid(1, 1, 550, currentTime + 1000, "extra"));
        bids.add(new Bid(2, 2, 350, currentTime + 6000, "extra"));
        bids.add(new Bid(3, 1, 250, currentTime + 11000, "extra"));
        bids.add(new Bid(1, 1, 560, currentTime + 500, "extra"));
        bids.add(new Bid(2, 2, 360, currentTime + 8000, "extra"));
        bids.add(new Bid(3, 1, 260, currentTime + 15000, "extra"));
        // add watermark to the stream
        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((bid, timestamp) -> bid.dateTime));

        // Make a keyed stream
        KeyedStream<Auction, Long> keyedAuctions = auctionStream.keyBy(new KeySelector<Auction, Long>() {
            @Override
            public Long getKey(Auction auction) throws Exception {
                return auction.category;
            }
        });

        KeyedStream<Bid, Long> keyedBids = bidDataStream.keyBy(new KeySelector<Bid, Long>() {
            @Override
            public Long getKey(Bid bid) throws Exception {
                return bid.auction;
            }
        });

        // Join the two streams
        DataStream<Tuple4<Long, Long, Long, Long>> joinedStream = keyedBids.connect(keyedAuctions)
                .process(new Query4.JoinBidsWithAuctions());

        DataStream<Tuple4<Long, Long, Long, Long>> maxBids = joinedStream
                .keyBy(value -> value.f3)
                .process(new Query4.MaxPriceBid());

        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Double>> averages = maxBids.keyBy(value -> value.f3)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new Query4.AverageAggregate());

        List<Tuple4<Long, Long, Long, Double>> results = averages.executeAndCollect(2);
        System.out.println("*************" + results + "*************");
        // expected results
        List<Tuple4<Long, Long, Long, Double>> expectedResults = Arrays.asList(
                new Tuple4<>(1L, 2L, 1110L, 555.0),
                new Tuple4<>(2L, 2L, 710L, 355.0)
        );
        assertEquals(expectedResults.size(), results.size());
        assertEquals(expectedResults, results);
    }

    public static class CollectSink implements SinkFunction<Tuple4<Long, Long, Long, Double>> {

        // Must be static because Flink will distribute Sink Functions across multiple tasks and nodes.
        public static final List<Tuple4<Long, Long, Long, Double>> values = new ArrayList<>();

        @Override
        public void invoke(Tuple4<Long, Long, Long, Double> value, Context context) throws Exception {
            values.add(value);

            System.out.println("*************" + value + "*************");
        }
    }


}
