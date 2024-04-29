package flink.test;

import flink.queries.Query6;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Test for Query6.
 *
 * @@author arkashjain
 */
/*
 * Query 6: Average Selling Price by Seller
 * SELECT Istream(AVG(Q.final), Q.seller)
 * FROM (SELECT Rstream(MAX(B.price) AS final, A.seller)
 *       FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
 *       WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
 *       GROUP BY A.id, A.seller) [PARTITION BY A.seller ROWS 10] Q
 * GROUP BY Q.seller;
 */
public class testQuery6Stateful {
    @Test
    public void testAverageInPartitionWithSeller() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool params = ParameterTool.fromArgs(new String[]{});
        List<Auction> auctions = new ArrayList<>();
        List<Bid> bids = new ArrayList<>();
        // Create auctions and bids
        auctions.add(new Auction(1, "Item1", "Description1", 10, 100, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 1, 1, ""));
        auctions.add(new Auction(2, "Item2", "Description2", 20, 200, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 2, 2, ""));
        auctions.add(new Auction(3, "Item3", "Description3", 30, 300, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 3, 3, ""));
        auctions.add(new Auction(4, "Item4", "Description4", 40, 400, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 4, 4, ""));
        auctions.add(new Auction(20, "Item20", "Description20", 200, 2000, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 2, 2, ""));
        auctions.add(new Auction(100, "Item100", "Description100", 1000, 10000, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 1, 1, ""));
        auctions.add(new Auction(200, "Item200", "Description200", 2000, 20000, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 2, 2, ""));
        auctions.add(new Auction(300, "Item300", "Description300", 3000, 30000, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 3, 3, ""));
        DataStream<Auction> auctionStream = env.fromCollection(auctions)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Auction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((auction, timestamp) -> auction.dateTime));
        bids.add(new Bid(1, 1, 20, System.currentTimeMillis(), ""));
        bids.add(new Bid(1, 2, 30, System.currentTimeMillis(), ""));
        bids.add(new Bid(2, 1, 40, System.currentTimeMillis(), ""));
        bids.add(new Bid(2, 2, 50, System.currentTimeMillis(), ""));
        bids.add(new Bid(3, 1, 60, System.currentTimeMillis(), ""));
        bids.add(new Bid(3, 2, 70, System.currentTimeMillis(), ""));
        bids.add(new Bid(4, 1, 80, System.currentTimeMillis(), ""));
        bids.add(new Bid(4, 2, 90, System.currentTimeMillis(), ""));
        bids.add(new Bid(20, 1, 200, System.currentTimeMillis(), ""));
        bids.add(new Bid(20, 2, 300, System.currentTimeMillis(), ""));
        bids.add(new Bid(100, 1, 1000, System.currentTimeMillis(), ""));
        bids.add(new Bid(100, 2, 2000, System.currentTimeMillis(), ""));
        DataStream<Bid> bidStream = env.fromCollection(bids).assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((bid, timestamp) -> bid.dateTime));
        KeyedStream<Auction, Long> auctionKeyedStream = auctionStream.keyBy(new KeySelector<Auction, Long>() {
            @Override
            public Long getKey(Auction auction) {
                return auction.seller;
            }
        });
        KeyedStream<Bid, Long> bidKeyedStream = bidStream.keyBy(new KeySelector<Bid, Long>() {
            @Override
            public Long getKey(Bid bid) {
                return bid.auction;
            }
        });

        // uncomment to view bids
//        bidKeyedStream.addSink(new SinkFunction<Bid>() {
//            @Override
//            public void invoke(Bid value, Context context) {
//                System.out.println("Bid: " + value.auction + ", Price: " + value.price);
//            }
//        });
        // uncomment to view auctions
//        auctionKeyedStream.addSink(new SinkFunction<Auction>() {
//            @Override
//            public void invoke(Auction value, Context context) {
//                System.out.println("Auction: " + value.id + ", Seller: " + value.seller);
//            }
//        });

        // (auction.id, bid.price, auction.expires, auction.seller)
        DataStream<Tuple4<Long, Long, Long, Long>> joinedStream = bidKeyedStream.connect(auctionKeyedStream)
                .process(new Query6.JoinBidsWithAuctions());

        // Keyby seller
        KeyedStream<Tuple4<Long, Long, Long, Long>, Long> keyedStream = joinedStream.keyBy(new KeySelector<Tuple4<Long, Long, Long, Long>, Long>() {
            @Override
            public Long getKey(Tuple4<Long, Long, Long, Long> value) {
                return value.f3;
            }
        });

        // public static class CalculateMaxBid extends KeyedProcessFunction<Long, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>>
        DataStream<Tuple4<Long, Long, Long, Long>> maxBidStream = keyedStream
                .process(new Query6.CalculateMaxBid())
                .name("CalculateMaxBid");

        maxBidStream.addSink(new SinkFunction<Tuple4<Long, Long, Long, Long>>() {
            @Override
            public void invoke(Tuple4<Long, Long, Long, Long> value, Context context) {
                System.out.println("value is: " + value);
                System.out.println("--------------------------------------");
            }
        });

        DataStream<Tuple2<Long, Double>> avgPriceStream = maxBidStream.keyBy(new KeySelector<Tuple4<Long, Long, Long, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple4<Long, Long, Long, Long> value) throws Exception {
                        return value.f3;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new Query6.AverageAggregate());

        avgPriceStream.addSink(new SinkFunction<Tuple2<Long, Double>>() {
            @Override
            public void invoke(Tuple2<Long, Double> value, Context context) {
                System.out.println("average is: " + value);
                System.out.println("--------------------------------------");

            }
        });

        // verification
        // (1,25.0), (2,45.0), (3,65.0), (4,85.0)

        List<Tuple2<Long, Double>> results = avgPriceStream.executeAndCollect(4);

        List<Tuple2<Long, Double>> expectedResults = new ArrayList<>();
        expectedResults.add(new Tuple2<>(1L, 25.0));
        expectedResults.add(new Tuple2<>(2L, 45.0));
        expectedResults.add(new Tuple2<>(3L, 65.0));
        expectedResults.add(new Tuple2<>(4L, 85.0));
        expectedResults.sort(Comparator.comparing(tuple -> tuple.f0));
        results.sort(Comparator.comparing(tuple -> tuple.f0));

        for (int i = 0; i < results.size(); i++) {
            Assert.assertEquals(expectedResults.get(i).f0, results.get(i).f0);
            Assert.assertEquals(expectedResults.get(i).f1, results.get(i).f1, 0.1);
        }
        assert (results.size() == 4);
    }

}
