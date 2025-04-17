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
        return consumerConfig;
    }

    protected static Properties getProducerProperties() {
        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, Settings.getKafka().getClientID());
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Settings.getKafka().getUrl());
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerConfig;
    }

}
