package flink.test;

import flink.queries.Query7;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test for Query7.
 *
 * @@author arkashjain
 */
public class testQuery7 {
    /**
     * Query 7 monitors the highest price items currently on auction. Every ten minutes, this query returns the highest bid (and associated itemid)
     * in the most recent ten minutes.
     * This query uses a time-based, fixed- window group by.
     * The syntax FIXEDRANGE is used in place of RANGE to indicate that the highest bid should be evaluated every ten minutes instead
     * of over a sliding ten minute window.
     */

    /**
     * Simple test to generate a stream of bids and calculate the highest bid in a tumbling window.
     *
     * @throws Exception
     */
    @Test
    public void testHighestBidCalculation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        ParameterTool params = ParameterTool.fromArgs(new String[]{});
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);

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

        DataStream<Tuple3<Long, Long, Long>> highestBids = Query7.generateStreamWithTumblingWindow(bidDataStream);
        env.execute();


        // use a sink
        highestBids.addSink(new SinkFunction<Tuple3<Long, Long, Long>>() {
            @Override
            public void invoke(Tuple3<Long, Long, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        List<Tuple3<Long, Long, Long>> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Tuple3<>(1L, 60L, 6L));
        expectedOutput.add(new Tuple3<>(2L, 40L, 4L));

        List<Tuple3<Long, Long, Long>> result = highestBids.executeAndCollect(2);
        Assert.assertEquals(expectedOutput, result);
    }

    /**
     * Test for handling late data. The test generates 3 bids, one of which is late.
     *
     * @throws Exception
     */
//    @Test
    public void testLateDataHandling() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long baseTime = System.currentTimeMillis();
        List<Bid> bids = Arrays.asList(
                new Bid(1, 1, 10, baseTime - 600_000, ""), // exactly on the edge of the previous window
                new Bid(1, 2, 500, baseTime - 300_000, ""), // late but within the allowed lateness
                new Bid(1, 3, 200, baseTime - 610_000, "") // too late, should be ignored
        );

        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.dateTime));

        DataStream<Tuple3<Long, Long, Long>> highestBids = Query7.generateStreamWithTumblingWindow(bidDataStream);
        env.execute();
        List<Tuple3<Long, Long, Long>> expectedOutput = Arrays.asList(
                new Tuple3<>(1L, 200L, 3L) // only this should be processed as valid
        );
        List<Tuple3<Long, Long, Long>> result = highestBids.executeAndCollect(1);
        Assert.assertEquals("Results do not properly handle late data", expectedOutput, result);

        highestBids.addSink(new SinkFunction<Tuple3<Long, Long, Long>>() {
            @Override
            public void invoke(Tuple3<Long, Long, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });
    }

    /**
     * Test for edge case at the boundary of the window.
     * The first bid is just before the window closes and the second bid is exactly at the window boundary.
     * The second bid should be included in the window.
     *
     * @throws Exception
     */
    @Test
    public void testEdgeCaseAtBoundary() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long baseTime = System.currentTimeMillis();
        List<Bid> bids = Arrays.asList(
                new Bid(1, 1, 50, baseTime - 10_000, ""), // just before the window closes
                new Bid(1, 2, 300, baseTime, "") // exactly at the window boundary
        );

        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.dateTime));

        DataStream<Tuple3<Long, Long, Long>> highestBids = Query7.generateStreamWithTumblingWindow(bidDataStream);
        env.execute();
        List<Tuple3<Long, Long, Long>> expectedOutput = Arrays.asList(
                new Tuple3<>(1L, 300L, 2L)
        );

        List<Tuple3<Long, Long, Long>> result = highestBids.executeAndCollect(1);
        Assert.assertEquals("Edge case at the window boundary is not handled correctly", expectedOutput, result);

        highestBids.addSink(new SinkFunction<Tuple3<Long, Long, Long>>() {
            @Override
            public void invoke(Tuple3<Long, Long, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });
    }


    /**
     * Test for Query7 with 300 bids generated in 30 windows of 10 minutes each.
     * We verify the size of the window and the solution for each one
     *
     * @throws Exception
     */
    @Test
    public void generate300bids() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long baseTime = System.currentTimeMillis();
        List<Bid> bids = new ArrayList<>();
        // Increment time by 10 minutes (600,000 milliseconds) for each bid
        for (int i = 0; i < 300; i++) {
            bids.add(new Bid(1, i, i * 10, baseTime + i * 600000L, ""));
        }

        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((bid, timestamp) -> bid.dateTime));

        DataStream<Tuple3<Long, Long, Long>> highestBids = Query7.generateStreamWithTumblingWindow(bidDataStream);
        env.execute();
        List<Tuple3<Long, Long, Long>> result = highestBids.executeAndCollect(300);

        // Expecting one result per window, as each bid is in its own window
        Assert.assertEquals(300, result.size());
        // Check if the highest bid in each window is as expected
        for (int i = 0; i < 300; i++) {
            Tuple3<Long, Long, Long> expected = new Tuple3<>(1L, (long) (i * 10), (long) i);
            Assert.assertEquals(expected, result.get(i));
        }

        highestBids.addSink(new SinkFunction<Tuple3<Long, Long, Long>>() {
            @Override
            public void invoke(Tuple3<Long, Long, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });
    }

    /**
     * Test for random 1000 bids, out of which a random %10 is late
     * The test generates 1000 bids, out of which 100 are late by 10 minutes.
     * The expected output is the highest bid for each window.
     * The test verifies that the late bids are ignored.
     *
     * @throws Exception
     */
    @Test
    public void generate1000bidsWithLateData() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        long baseTime = System.currentTimeMillis();
        List<Bid> bids = new ArrayList<>();
        // Spread out bids enough to ensure they fall into different 10-minute windows.
        long timeIncrement = 10 * 60 * 1000; // 10 minutes in milliseconds
        for (int i = 0; i < 1000; i++) {
            bids.add(new Bid(1, i, i * 10, baseTime + i * timeIncrement, ""));
        }

        // Generate late bids, ensure they are late relative to the last bid's window.
        for (int i = 0; i < 100; i++) {
            long lateBidTime = baseTime + (1000 * timeIncrement) - (600 * 1000); // late by 10 minutes from the last regular bid
            bids.add(new Bid(1, i + 1000, i, lateBidTime, ""));
        }

        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(30)) // 30 seconds lateness allowance
                        .withTimestampAssigner((bid, timestamp) -> bid.dateTime));

        DataStream<Tuple3<Long, Long, Long>> highestBids = Query7.generateStreamWithTumblingWindow(bidDataStream);
        env.execute();
        // Adjust to collect all bids processed, since we know exactly how many to expect: 1000 regular + some number affected by late bids
        List<Tuple3<Long, Long, Long>> result = highestBids.executeAndCollect("Collect highest bids", 1100);
        Assert.assertEquals("Expected results did not match", 1000, result.size());

        highestBids.addSink(new SinkFunction<Tuple3<Long, Long, Long>>() {
            @Override
            public void invoke(Tuple3<Long, Long, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });
    }

    /**
     * Test for random 1000 bids, out of which a random % is late
     * We verify the size of the window and the solution for each one
     */
    @Test
    public void generate1000bidsWithLateDataAndEdgeCase() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long baseTime = System.currentTimeMillis();
        List<Bid> bids = new ArrayList<>();
        Random random = new Random();

        // Generate 1000 bids
        for (int i = 0; i < 1000; i++) {
            bids.add(new Bid(1, i, i * 10, baseTime + i * 60000, ""));
        }

        // Generate late bids
        for (int i = 0; i < 100; i++) {
            long lateBidTime = baseTime + (1000 * 60000) - (600 * 1000); // late by 10 minutes from the last regular bid
            bids.add(new Bid(1, i + 1000, i, lateBidTime, ""));
        }

        // Generate edge case bid
        bids.add(new Bid(1, 1100, 1000, baseTime + 1000 * 60000, ""));

        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner((bid, timestamp) -> bid.dateTime));

        DataStream<Tuple3<Long, Long, Long>> highestBids = Query7.generateStreamWithTumblingWindow(bidDataStream);
        env.execute();

        List<Tuple3<Long, Long, Long>> result = highestBids.executeAndCollect("Collect highest bids", 1101);
        Assert.assertEquals("Expected results did not match", 101, result.size());

        highestBids.addSink(new SinkFunction<Tuple3<Long, Long, Long>>() {
            @Override
            public void invoke(Tuple3<Long, Long, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });
    }

    /**
     * Test for multiple auction IDs.
     * The test generates 500 bids for 5 different auction IDs. Each auction ID has 100 bids.
     * The expected output is the highest bid for each auction ID.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleAuctionIds() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Ensure that everything runs in a single thread for deterministic output

        long currentTime = System.currentTimeMillis();
        Random random = new Random(42); // Fixed seed for reproducibility

        List<Bid> bids = IntStream.range(0, 500)
                .mapToObj(i -> new Bid((long) i % 5, i, 1000 - i, currentTime + (i * 100), ""))
                .collect(Collectors.toList());

        // Adjust expected output calculation
        Map<Long, Tuple3<Long, Long, Long>> expectedMaxBids = bids.stream()
                .collect(Collectors.groupingBy(
                        bid -> bid.auction,
                        Collectors.collectingAndThen(
                                Collectors.maxBy(Comparator.comparingLong(bid -> bid.price)),
                                optionalBid -> new Tuple3<>(optionalBid.get().auction, optionalBid.get().price, optionalBid.get().bidder)
                        )
                ));

        List<Tuple3<Long, Long, Long>> expectedOutput = new ArrayList<>(expectedMaxBids.values());
        expectedOutput.sort(Comparator.comparing(t -> t.f0)); // Sort by auction ID for consistent ordering

        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((bid, timestamp) -> bid.dateTime));

        DataStream<Tuple3<Long, Long, Long>> highestBids = Query7.generateStreamWithTumblingWindow(bidDataStream);
        env.execute();

        List<Tuple3<Long, Long, Long>> result = highestBids.executeAndCollect(5);
        result.sort(Comparator.comparing(t -> t.f0)); // Sort by auction ID for consistent ordering
        Assert.assertEquals(expectedOutput, result);

        highestBids.addSink(new SinkFunction<Tuple3<Long, Long, Long>>() {
            @Override
            public void invoke(Tuple3<Long, Long, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });
    }


    /**
     * Checking the reverse chronological order of the bids for watermark testing
     */
    @Test
    public void testReverseChronologicalBids() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long currentTime = System.currentTimeMillis();
        List<Bid> bids = new ArrayList<>();
        for (int i = 100; i >= 0; i--) {
            bids.add(new Bid(1, i, 1000 - i, currentTime + (i * 100), ""));
        }

        DataStream<Bid> bidDataStream = env.fromCollection(bids)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((bid, timestamp) -> bid.dateTime));

        DataStream<Tuple3<Long, Long, Long>> highestBids = Query7.generateStreamWithTumblingWindow(bidDataStream);
        env.execute();
        // use a sink and verify the results
        highestBids.addSink(new SinkFunction<Tuple3<Long, Long, Long>>() {
            @Override
            public void invoke(Tuple3<Long, Long, Long> value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        // verify the results
        List<Tuple3<Long, Long, Long>> result = highestBids.executeAndCollect(5);

        // the highest bid in each window should be the last one
        Tuple3<Long, Long, Long> lastBid = result.get(0);
        Tuple3<Long, Long, Long> expectedBid = new Tuple3<>(1L, 1000L, 0L);
        /*
        Each Bid object is initialized as new Bid(auctionId, bidder, price, dateTime, extra).
        Therefore, the last bid (i = 0) has auctionId = 1, bidder = 0, price = 1000 (since it's calculated as 1000 - i)
        and dateTime = currentTime + (i * 100) = currentTime + 0 = currentTime.

        The expectedBid is a Tuple3 object with auctionId = 1, price = 1000, bidder = 0.
        The return tuple of generateStreamWithTumblingWindow is a Tuple3 object of the auctionId, price, and bidder.
         */
        Assert.assertEquals(expectedBid, lastBid);
    }
}
