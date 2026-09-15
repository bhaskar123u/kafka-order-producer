package com.bsharan.kafka_order_producer.controllers;

import com.bsharan.kafka_order_producer.models.Order;
import com.bsharan.kafka_order_producer.services.OrderProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    @Autowired
    OrderProducerService orderProducerService;

    @PostMapping("/with-key")
    public ResponseEntity<String> sendWithKey(@RequestBody Order order) {
        orderProducerService.sendWithKey(order);
        return ResponseEntity.accepted()
                .body("Order created and event published");
    }

    @PostMapping("/with-no-key")
    public ResponseEntity<String> sendWithoutKey(@RequestBody Order order) {
        orderProducerService.sendWithoutKey(order);
        return ResponseEntity.accepted()
                .body("Order created and event published");
    }

    @PostMapping("/with-no-key/{count}")
    public ResponseEntity<String> sendMultipleWithoutKey(@PathVariable int count) {
        orderProducerService.sendBatchWithoutKey(count);
        return ResponseEntity.accepted()
                .body(count + " orders published to Kafka");
    }
}