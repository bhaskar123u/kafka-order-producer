package com.bsharan.kafka_order_producer.configs;

import com.bsharan.kafka_order_producer.models.Order;
import com.bsharan.kafka_order_producer.serializer.OrderSerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic orderEventsTopic(){
        return TopicBuilder.name("order-events")
                .partitions(3)
                .replicas(2)
                .config(TopicConfig.RETENTION_MS_CONFIG,"604800000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG,"delete")
                // for this topic, I require at least 2 replicas to be in the ISR for an acks=all write to succeed.
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,"2")
                .config(TopicConfig.SEGMENT_BYTES_CONFIG,"104857600")
                .build();
        // these same configs we can pass in /configs folder
    }

    @Bean
    public KafkaTemplate<String, Order> customSerializerKafkaTemplate(KafkaProperties kafkaProperties){
        // start with all the producer properties in application.properties
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        // override ONLY the value serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class);
        DefaultKafkaProducerFactory<String, Order> factory = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }
}
