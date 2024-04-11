package flink.kafkaSources;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.sources.PersonSourceFunction;
import flink.utils.PersonSchema;
import org.apache.beam.sdk.nexmark.model.Person;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KafkaSourcePerson {
    public static void main(String[] args) throws Exception {
        System.out.println("Options for both the above setups: ");
        System.out.println("\t[--kafka-topic <topic>]");
        System.out.println("\t[--broker <broker>]");
        System.out.println("\t[--ratelist <ratelist>]");
        System.out.println();

        // Checking input parameters
        //  --broker 192.168.1.180:9092 --kafka-topic query3  --ratelist 10000_300000_200_600000
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String broker = params.getRequired("broker");
        final String kafkaTopic = params.getRequired("kafka-topic");
        System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, broker);
        System.out.println();
        String ratelist = params.getRequired("ratelist");
        int[] numbers = Arrays.stream(ratelist.split("_")).mapToInt(Integer::parseInt).toArray();
        System.out.println(Arrays.toString(numbers));
        List<List<Integer>> personSrcRates = new ArrayList<>();
        for (int i = 0; i < numbers.length - 1; i += 2) {
            personSrcRates.add(Arrays.asList(numbers[i], numbers[i + 1]));
        }

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> persons =
                env.addSource(new PersonSourceFunction(personSrcRates))
                        .name("Custom Source: Persons")
                        .setParallelism(params.getInt("p-person-source", 1));

        // write the filtered data to a Kafka sink
        KafkaSink<Person> sink =
                KafkaSink.<Person>builder()
                        .setBootstrapServers(broker)
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(kafkaTopic)
                                        .setValueSerializationSchema(new PersonSchema())
                                        .build())
                        .build();

        persons.sinkTo(sink);
        // run the cleansing pipeline
        env.execute("Person Events to Kafka");
    }
}
