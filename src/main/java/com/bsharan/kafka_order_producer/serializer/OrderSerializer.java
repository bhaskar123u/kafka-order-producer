package com.bsharan.kafka_order_producer.serializer;

import com.bsharan.kafka_order_producer.models.Order;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public class OrderSerializer implements Serializer<Order> {

    private final ObjectMapper objectMapper;

    public OrderSerializer(){
        this.objectMapper = new ObjectMapper();
    }

//    @Override
//    public byte[] serialize(String topic, Order order) {
//        if (order == null)
//            return null;
////        String value =
////                order.getOrderId() + "|" +
////                        order.getCustomerId() + "|" +
////                        order.getProductId() + "|" +
////                        order.getQuantity() + "|" +
////                        order.getTotalAmount() + "|" +
////                        order.getStatus();
////        return value.getBytes(StandardCharsets.UTF_8);
//        try{
//            // hide sensitive data and convert only useful data
//            Map<String, Object> summary = new LinkedHashMap<>();
//            summary.put("orderId", order.getOrderId());
//            summary.put("customerId", order.getCustomerId());
//            summary.put("productId", order.getProductId());
//            return objectMapper.writeValueAsBytes(summary);
//        } catch (Exception ex){
//            throw new RuntimeException("Failed to serialise order summary",ex);
//        }
//    }

    @Override
    public byte[] serialize(String topic, Order order) {

        if (order == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(order);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to serialize order", ex);
        }
    }
}
