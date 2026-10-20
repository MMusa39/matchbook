package dev.matchbook.posttrade;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

@Configuration
public class KafkaConfig {

    /**
     * Transient failures (database briefly down) are retried with backoff for about 30 seconds; a record
     * that still fails, or one that cannot be parsed, goes to {@code <topic>.DLT} instead of blocking its
     * partition forever. The dead-letter producer writes the original bytes unchanged, so it has its own
     * byte-array serializer: the application's producer serialises strings.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaProperties properties) {
        Map<String, Object> config = properties.buildProducerProperties();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        KafkaTemplate<String, byte[]> dltTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(dltTemplate,
                (record, e) -> new TopicPartition(record.topic() + ".DLT", -1));
        ExponentialBackOff backOff = new ExponentialBackOff(200, 2.0);
        backOff.setMaxInterval(5_000);
        backOff.setMaxElapsedTime(30_000);
        DefaultErrorHandler handler = new DefaultErrorHandler(dlt, backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class,
                com.google.protobuf.InvalidProtocolBufferException.class);
        return handler;
    }
}
