package com.bsharan.kafka_order_producer.serializer;

import com.bsharan.kafka_order_producer.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class LoggingAvroSerializer implements Serializer<Order> {

    private final KafkaAvroSerializer delegate = new KafkaAvroSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Order data) {
        byte[] bytes = delegate.serialize(topic, data);
        logSerializedData(data, bytes);
        return bytes;
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Order data) {
        byte[] bytes = delegate.serialize(topic, headers, data);
        logSerializedData(data, bytes);
        return bytes;
    }

    private void logSerializedData(Order order, byte[] bytes) {

        int schemaId = ByteBuffer.wrap(bytes, 1, 4).getInt();

        System.out.println("\n========== AVRO SERIALIZATION ==========");
        System.out.println("Order       : " + order);
        System.out.println("Total bytes : " + bytes.length);
        System.out.println("Magic byte  : " + bytes[0]);
        System.out.println("Schema ID   : " + schemaId);
        System.out.println("Raw HEX     : " + toHex(bytes)); // EVERYTHING Kafka receives as the value

        byte[] payload = new byte[bytes.length - 5];
        System.arraycopy(bytes, 5, payload, 0, payload.length);

        System.out.println("Avro payload: " + toHex(payload));
        System.out.println("========================================\n");
    }

    private String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();

        for (byte b : bytes) {
            result.append(String.format("%02X ", b));
        }

        return result.toString().trim();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
