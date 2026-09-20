package com.bsharan.kafka_order_producer.services;

import com.bsharan.kafka_order_producer.models.Order;
import org.apache.kafka.common.errors.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.bsharan.kafka_order_producer.constants.AppConstants.characters;

@Service
public class OrderProducerService {

    // picks values from application.properties
    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    // picks values from configuration annotated with @Bean
    @Autowired
    @Qualifier("customSerializerKafkaTemplate")
    private KafkaTemplate<String, Order> customSerializerKafkaTemplate;

    private static final List<String> PRODUCT_IDS = IntStream.rangeClosed(1, 100)
            .mapToObj(i -> String.format("P%03d", i))
            .toList();

    public void sendWithKey(Order order){
        String key = order.getOrderId();

        CompletableFuture<SendResult<String, Order>> future = kafkaTemplate.send("order-events",key,order);

        // whenever the sender thread sends response back from broker
        future.whenComplete((result, exception)->{
            if(exception == null){
                System.out.println("Order event sent successfully");
                System.out.println("Partition: "+result.getRecordMetadata().partition());
                System.out.println("Offset: "+result.getRecordMetadata().offset());
            }else{
                System.out.println("Failed to send message: "+exception.getMessage());
            }
        });
    }

    public void sendWithoutKey(Order order){
        String key = order.getOrderId();

        CompletableFuture<SendResult<String, Order>> future = customSerializerKafkaTemplate.send("order-events",order);

        // whenever the sender thread sends response back from broker
        future.whenComplete((result, exception)->{
            if(exception == null){
                System.out.println("Order event sent successfully");
                System.out.println("Partition: "+result.getRecordMetadata().partition());
                System.out.println("Offset: "+result.getRecordMetadata().offset());
            }else{
                System.out.println("Failed to send message: "+exception.getMessage());
            }
        });
    }

    public void sendBatchWithoutKey(int count) {

        for (int i = 0; i < count; i++) {
            Order order = generateOrder();
            CompletableFuture<SendResult<String, Order>> future =
                    customSerializerKafkaTemplate.send("order-events", order);
            future.whenComplete((result, exception) -> {
                // Successful send
                if (exception == null) {
                    System.out.println("\u001B[31mSUCCESS | partition=" + result.getRecordMetadata().partition() + " | offset=" + result.getRecordMetadata().offset() + "\u001B[0m");
                    return;
                }
                // Spring Kafka may wrap the actual Kafka exception
                Throwable kafkaException = exception.getCause();
                String errorMessage = switch (kafkaException) {
                    case NotEnoughReplicasException e ->
                            "ERROR: Not enough in-sync replicas";
                    case NotEnoughReplicasAfterAppendException e ->
                            "ERROR: Record appended, but not enough replicas acknowledged";
                    case TimeoutException e ->
                            "ERROR: Kafka request timed out";
                    case RecordTooLargeException e ->
                            "ERROR: Kafka record is too large";
                    case UnknownTopicOrPartitionException e ->
                            "ERROR: Topic or partition does not exist";
                    case NotLeaderOrFollowerException e ->
                            "ERROR: Broker is no longer the correct leader/follower";
                    default ->
                            "ERROR: Unknown Kafka error: " +
                                    kafkaException.getClass().getSimpleName();
                };

                System.out.println("FAILED | " + errorMessage + " | wrapper=" +
                exception.getClass().getSimpleName() + " | cause=" + kafkaException.getClass().getSimpleName());
                System.out.println("Kafka message: " + kafkaException.getMessage());
            });
        }
    }
    /*
        Controller
            │
            ▼
        sendBatchWithoutKey(10)
            │
            ├── send(Order-1) ──► RecordAccumulator
            ├── send(Order-2) ──► RecordAccumulator
            ├── send(Order-3) ──► RecordAccumulator
            ├── ...
            └── send(Order-10) ─► RecordAccumulator
                                      │
                                      ▼
                                 Sender Thread
                                      │
                                      ▼
                                ProduceRequest
                                      │
                                      ▼
                                   Broker
                                      │
                                      ▼
                               CompletableFuture
                                      │
                                      ▼
                                 whenComplete()
    */

    public Order generateOrder() {
        Order order = new Order();
        order.setOrderId(generateSixDigitNumber());
        order.setCustomerId(generateAlphaNumeric(10));
        // Pick a random product from the fixed list of 100 products
        String productId = PRODUCT_IDS.get((int) (Math.random() * PRODUCT_IDS.size()));
        order.setProductId(productId);
        order.setQuantity((int) (Math.random() * 5) + 1);
        order.setTotalAmount((double) (Math.random() * 5000) + 100);
        order.setStatus("CREATED");
        return order;
    }

    public String generateSixDigitNumber() {
        return String.valueOf(
                100000 + (int) (Math.random() * 900000)
        );
    }

    public String generateAlphaNumeric(int length) {

        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = (int) (Math.random() * characters.length());
            result.append(characters.charAt(index));
        }
        return result.toString();
    }
}
