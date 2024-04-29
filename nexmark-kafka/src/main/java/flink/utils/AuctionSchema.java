package flink.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class AuctionSchema implements DeserializationSchema<Auction>, SerializationSchema<Auction> {

    @Override
    public Auction deserialize(byte[] message) {
        Auction auction = null;
        try {
            // according to Bid.toString()
            auction = NexmarkUtils.MAPPER.readValue(message, Auction.class);
            // System.out.println(auction.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return auction;
    }

    @Override
    public boolean isEndOfStream(Auction nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Auction element) {
        try {
            // according to Bid.toString()
            return NexmarkUtils.MAPPER.writeValueAsBytes(element);
        } catch (JsonProcessingException var2) {
            throw new RuntimeException(var2);
        }
    }

    @Override
    public TypeInformation<Auction> getProducedType() {
        return TypeInformation.of(Auction.class);
    }
}
