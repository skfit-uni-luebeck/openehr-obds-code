package de.uksh.medic.etl;

import de.uksh.medic.etl.settings.KafkaSettings;
import de.uksh.medic.etl.settings.Settings;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaUtils {

    private KafkaUtils() {
    }

    protected static Properties getConsumerProperties() {
        KafkaSettings settings = Settings.getKafka();
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, settings.getClientID());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, settings.getGroup());
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getUrl());
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, settings.getOffset());
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.getPollRecords());
        // 2min max intervall to process getPollRecords() bundle
        consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000");
        // Session timeout: consumer is considered dead after this long without heartbeat
        consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // Heartbeat interval: should be 1/3 of session timeout
        consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        // Prevent premature connection closes (default is 9min, increase for safety)
        consumerConfig.setProperty(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "540000");
        // Reconnect backoff: exponential backoff when reconnecting to brokers
        consumerConfig.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000");
        consumerConfig.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        // Retry backoff for metadata requests
        consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "300000");
        return consumerConfig;
    }

    protected static Properties getProducerProperties() {
        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, Settings.getKafka().getClientID());
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Settings.getKafka().getUrl());
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Producer retries: automatically retry failed sends
        producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        // Backoff between retry attempts
        producerConfig.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        // Max time to wait for broker response
        producerConfig.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        // Max time to wait for metadata
        producerConfig.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        return producerConfig;
    }

}
