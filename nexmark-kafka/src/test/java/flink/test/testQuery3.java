package flink.test;

import flink.queries.Query3Fix;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for Query3Stateful.
 *
 * @@author arkashjain
 */
public class testQuery3 {

    /**
     * Test Query3.
     * Generate a stream of persons and auctions and join them to get the result.
     * We have over 100 auctions and 15 persons, where each person has a unique id and each auction has a unique id generated randomly.
     * The auctions are grouped by the seller and the persons are grouped by their id.
     *
     * @throws Exception
     */
    @Test
    public void testQuery3() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(new String[]{});
        List<Person> personList = new ArrayList<>();
        // Generate a list of persons from WA, OR, and CA
        String[] states = {"WA", "OR", "CA"};
        personList.add(new Person(1, "Alice", "", "", "Seattle", "WA", System.currentTimeMillis(), ""));
        personList.add(new Person(2, "Bob", "", "", "Portland", "OR", System.currentTimeMillis(), ""));
        personList.add(new Person(3, "Charlie", "", "", "San Francisco", "CA", System.currentTimeMillis(), ""));
        personList.add(new Person(4, "David", "", "", "Seattle", "WA", System.currentTimeMillis(), ""));
        personList.add(new Person(5, "Eve", "", "", "Portland", "OR", System.currentTimeMillis(), ""));
        personList.add(new Person(6, "Frank", "", "", "San Francisco", "CA", System.currentTimeMillis(), ""));
        personList.add(new Person(7, "Grace", "", "", "Seattle", "WA", System.currentTimeMillis(), ""));
        personList.add(new Person(8, "Heidi", "", "", "Portland", "OR", System.currentTimeMillis(), ""));
        personList.add(new Person(9, "Ivan", "", "", "San Francisco", "CA", System.currentTimeMillis(), ""));
        personList.add(new Person(10, "Judy", "", "", "Seattle", "WA", System.currentTimeMillis(), ""));
        personList.add(new Person(11, "Kevin", "", "", "Portland", "OR", System.currentTimeMillis(), ""));
        personList.add(new Person(12, "Larry", "", "", "San Francisco", "CA", System.currentTimeMillis(), ""));
        personList.add(new Person(13, "Mallory", "", "", "Seattle", "WA", System.currentTimeMillis(), ""));
        personList.add(new Person(14, "Nancy", "", "", "Portland", "OR", System.currentTimeMillis(), ""));
        personList.add(new Person(15, "Oscar", "", "", "San Francisco", "CA", System.currentTimeMillis(), ""));

        DataStream<Person> personStream = env.fromCollection(personList);
        KeyedStream<Person, Long> keyedPersons =
                personStream.keyBy(new KeySelector<Person, Long>() {
                    @Override
                    public Long getKey(Person person) throws Exception {
                        return person.id;
                    }
                });
        List<Auction> auctionList = new ArrayList<>();
        Random random = new Random();
        // Generate a list of auctions with random values
        for (int i = 0; i < 100; i++) {
            auctionList.add(new Auction(i, "item" + i, "description" + i, random.nextInt(90) + 10,
                    random.nextInt(90) + 10, System.currentTimeMillis() + 1000, System.currentTimeMillis() + 5000, random.nextInt(10) + 1,
                    random.nextInt(15) + 1, "extra"));
        }
        DataStream<Auction> auctionStream = env.fromCollection(auctionList);
        KeyedStream<Auction, Long> keyedAuctions =
                auctionStream.keyBy(new KeySelector<Auction, Long>() {
                    @Override
                    public Long getKey(Auction auction) throws Exception {
                        return auction.seller;
                    }
                });

        DataStream<Tuple4<String, String, String, Long>> result = Query3Fix.executeQuery3(keyedAuctions, keyedPersons);

        // Print the result using a sink
        result.addSink(new SinkFunction<Tuple4<String, String, String, Long>>() {
            @Override
            public void invoke(Tuple4<String, String, String, Long> value, Context context) throws Exception {
                System.out.println("***************" + value + "***************");
            }
        });

        env.execute();
    }

    @Test
    public void testCategoryAndStateFiltering() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Set the parallelism to 1 to keep the output deterministic
        env.setParallelism(1);

        // Generate a list of persons from WA, OR, and CA
        List<Person> personList = Arrays.asList(
                new Person(1, "Alice", "", "", "Portland", "OR", System.currentTimeMillis(), ""),
                new Person(2, "Bob", "", "", "Boise", "ID", System.currentTimeMillis(), ""),
                new Person(3, "Charlie", "", "", "San Diego", "CA", System.currentTimeMillis(), ""),
                new Person(4, "David", "", "", "Reno", "NV", System.currentTimeMillis(), "")
        );

        DataStream<Person> personStream = env.fromCollection(personList);
        KeyedStream<Person, Long> keyedPersons =
                personStream.keyBy(new KeySelector<Person, Long>() {
                    @Override
                    public Long getKey(Person person) throws Exception {
                        return person.id;
                    }
                });

        // Generate a list of auctions with random values
        List<Auction> auctionList = Arrays.asList(
                new Auction(1, "item1", "description1", 20, 30, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 1, 10, "extra"),
                new Auction(2, "item2", "description2", 20, 30, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 3, 9, "extra"), // Should not be included
                new Auction(3, "item3", "description3", 20, 30, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 2, 10, "extra"),
                new Auction(4, "item4", "description4", 20, 30, System.currentTimeMillis(), System.currentTimeMillis() + 10000, 4, 10, "extra")  // Should not be included
        );

        DataStream<Auction> auctionStream = env.fromCollection(auctionList);
        KeyedStream<Auction, Long> keyedAuctions = auctionStream.keyBy(new KeySelector<Auction, Long>() {
            @Override
            public Long getKey(Auction auction) throws Exception {
                return auction.seller;
            }
        });

        DataStream<Tuple4<String, String, String, Long>> result = Query3Fix.executeQuery3(keyedAuctions, keyedPersons);

        List<Tuple4<String, String, String, Long>> output = result.executeAndCollect(4);
        System.out.println("==========" + output + "=============");

        // Only Alice and Bob should be included
        assertEquals(2, output.size());
        assertTrue(output.contains(new Tuple4<>("Alice", "Portland", "OR", 1L)));
        assertTrue(output.contains(new Tuple4<>("Bob", "Boise", "ID", 3L)));
    }
}
