package flink.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;

public class BidSchema implements DeserializationSchema<Bid>, SerializationSchema<Bid> {

    @Override
    public Bid deserialize(byte[] message) {
        Bid bid = null;
        try {
            // according to Bid.toString()
            bid = NexmarkUtils.MAPPER.readValue(message, Bid.class);
            // System.out.println(bid.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bid;
    }

    @Override
    public boolean isEndOfStream(Bid nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Bid element) {
        try {
            // according to Bid.toString()
            return NexmarkUtils.MAPPER.writeValueAsBytes(element);
        } catch (JsonProcessingException var2) {
            throw new RuntimeException(var2);
        }
    }

    @Override
    public TypeInformation<Bid> getProducedType() {
        return TypeInformation.of(Bid.class);
    }
}
