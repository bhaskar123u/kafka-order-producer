package com.bsharan.kafka_order_producer.configs;

// import com.bsharan.kafka_order_producer.models.Order;
import com.bsharan.kafka_order_producer.avro.Order;
import com.bsharan.kafka_order_producer.serializer.OrderSerializer;
import com.bsharan.kafka_order_producer.serializer.LoggingAvroSerializer;
// import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.springframework.kafka.core.ProducerFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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

    /*
    @Bean
    public KafkaTemplate<String, Order> customSerializerKafkaTemplate(KafkaProperties kafkaProperties){
        // start with all the producer properties in application.properties
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        // override ONLY the value serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class);
        DefaultKafkaProducerFactory<String, Order> factory = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }
    */

    @Bean
    public ProducerFactory<String, Order> producerFactory(KafkaProperties kafkaProperties){
        // start with all the producer properties in application.properties
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LoggingAvroSerializer.class);

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        props.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 300000);

        DefaultKafkaProducerFactory<String, Order> factory =
                new DefaultKafkaProducerFactory<>(props);

        return factory;
    }

    @Bean
    public KafkaTemplate<String, Order> kafkaTemplate(
            ProducerFactory<String, Order> producerFactory){
        return new KafkaTemplate<>(producerFactory);
    }
}
