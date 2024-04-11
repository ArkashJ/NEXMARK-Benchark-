package flink.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Person;

public class PersonSchema implements DeserializationSchema<Person>, SerializationSchema<Person> {

    @Override
    public Person deserialize(byte[] message) {
        Person person = null;
        try {
            // according to Bid.toString()
            person = NexmarkUtils.MAPPER.readValue(message, Person.class);
            // System.out.println(person.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return person;
    }

    @Override
    public boolean isEndOfStream(Person nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Person element) {
        try {
            // according to Bid.toString()
            return NexmarkUtils.MAPPER.writeValueAsBytes(element);
        } catch (JsonProcessingException var2) {
            throw new RuntimeException(var2);
        }
    }

    @Override
    public TypeInformation<Person> getProducedType() {
        return TypeInformation.of(Person.class);
    }
}
