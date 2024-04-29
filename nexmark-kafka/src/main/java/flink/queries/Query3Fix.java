package flink.queries;

import flink.sinks.DummyLatencyCountingSink;
import flink.utils.AuctionSchema;
import flink.utils.PersonSchema;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SELECT Istream(P.name, P.city, P.state, A.id)
 * FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
 * WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category = 10;
 */
public class Query3Fix {
    private static final Logger logger = LoggerFactory.getLogger(Query3Fix.class);

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final String Broker = params.getRequired("broker");
        final String auctionKafkaTopic = params.getRequired("auction-kafka-topic");
        final String auctionKafkaGroup = params.getRequired("auction-kafka-group");
        final String personKafkaTopic = params.getRequired("person-kafka-topic");
        final String personKafkaGroup = params.getRequired("person-kafka-group");
        // --broker

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setLatencyTrackingInterval(5000);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(4);

        // Create Kafka source for Auction
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
        // Create Kafka source for Person
        KafkaSource<Person> personKafkaSource =
                KafkaSource.<Person>builder()
                        .setBootstrapServers(Broker)
                        .setGroupId(personKafkaGroup)
                        .setTopics(personKafkaTopic)
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(new PersonSchema()))
                        .setProperty(
                                KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "true")
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .build();

        // Create DataStream from Kafka source for Auction
        DataStream<Auction> auctions = env.fromSource(
                auctionKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Auction Kafka source");

        // Create DataStream from Kafka source for Person
        DataStream<Person> persons = env.fromSource(
                personKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Person Kafka source");

        // Key auction by seller
        KeyedStream<Auction, Long> keyedAuctions = auctions.keyBy(new KeySelector<Auction, Long>() {
            @Override
            public Long getKey(Auction auction) throws Exception {
                return auction.seller;
            }
        });

        // Key person by id
        KeyedStream<Person, Long> keyedPersons = persons.keyBy(new KeySelector<Person, Long>() {
            @Override
            public Long getKey(Person person) throws Exception {
                return person.id;
            }
        });

        // Join the two streams on seller = id and implement the filter such that state = OR or ID or CA and category = 10
        ConnectedStreams<Auction, Person> connectedStreams = keyedAuctions.connect(keyedPersons);
        DataStream<Tuple4<String, String, String, Long>> result = connectedStreams.flatMap(new JoinAuctionWithPerson());

        // write the filtered data to a DummyLatencyCountingSink
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        result.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-sink", 8));

        // execute program
        env.execute("Nexmark Query3");
    }

    // pass in keyedAuctions and keyedPersons
    public static DataStream<Tuple4<String, String, String, Long>> executeQuery3(KeyedStream<Auction, Long> keyedAuctions, KeyedStream<Person, Long> keyedPersons) {
        // Join the two streams on seller = id and implement the filter such that state = OR or ID or CA and category = 10
        ConnectedStreams<Auction, Person> connectedStreams = keyedAuctions.connect(keyedPersons);
        DataStream<Tuple4<String, String, String, Long>> result = connectedStreams.flatMap(new JoinAuctionWithPerson());

        System.out.println("Printing result to stdout. Use DummyLatencyCountingSink to measure latency.");
        return result;
    }


    /**
     * Join the two streams on seller = id and implement the filter such that state = OR or ID or CA and category = 10
     *
     * @return DataStream<Tuple4 < String, String, String, Long>> - Tuple of name, city, state, auction id
     * @throws Exception
     */
    private static class JoinAuctionWithPerson extends RichCoFlatMapFunction<Auction, Person, Tuple4<String, String, String, Long>> {
        // State to hold the latest Person object
        private ValueState<Person> personState;

        // State to hold the latest Auction object
        private ValueState<Auction> auctionState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Person> personDescriptor = new ValueStateDescriptor<>(
                    "person-state", // state name
                    Person.class // type information
            );
            personState = getRuntimeContext().getState(personDescriptor);

            ValueStateDescriptor<Auction> auctionDescriptor = new ValueStateDescriptor<>(
                    "auction-state", // state name
                    Auction.class);
            auctionState = getRuntimeContext().getState(auctionDescriptor);
        }

        /**
         * @param auction The stream element
         * @param out     The collector to emit resulting elements to
         * @throws Exception
         * @@Description: Update the state with the latest Auction object and emit the result if the conditions is satisfied
         * which are that the category is 10 and the state is OR or ID or CA
         */

        @Override
        public void flatMap1(Auction auction, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
            Long category = auction.category;
            Person person = personState.value();
//            System.out.println("Auction Category: " + auction.category);
//            if (person != null) {
//                System.out.println("Person's state is: " + person.state);
//            }
//            System.out.println("-----------------------HERE_------------------------------------");
            auctionState.update(auction);

            // filter the results by the category and see if the person is null or not
            if (person != null && auction.category == 10) {
                if (person.state != null) {
                    if (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA")) {

//                        System.out.println("-----------------"
//                                + "Name: " + person.name
//                                + "City: " + person.city
//                                + "State: " + person.state
//                                + "Auction ID: " + auction.id
//                                + "-----------------" );
                        out.collect(new Tuple4<>(person.name, person.city, person.state, auction.id));
                    }
                }
//                if (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA")) {
//                    out.collect(new Tuple4<>(person.name, person.city, person.state, auction.id));
//                }
            }
        }

        /**
         * @param person The stream element
         * @param out    The collector to emit resulting elements to
         * @throws Exception
         * @@Description: Update the state with the latest Person object and emit the result if the conditions is satisfied
         */
        @Override
        public void flatMap2(Person person, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
            personState.update(person);
            Auction auction = auctionState.value();
            if (auction != null && auction.category == 10) {
                if (person.state.equals("OR") || person.state.equals("ID") || person.state.equals("CA")) {
                    out.collect(new Tuple4<>(person.name, person.city, person.state, auction.id));
                }
            }
        }
    }


}

