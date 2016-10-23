package rampup.nosql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rampup.nosql.kafka.PutDataToKafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class FromKafkaToCassandra {

    private static Logger logger = LoggerFactory.getLogger(FromKafkaToCassandra.class);

    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("group.id", "visitsGroup");
        /*kafkaProperties.put("enable.auto.commit", "true");
        kafkaProperties.put("auto.commit.interval.ms", "1000");
        kafkaProperties.put("session.timeout.ms", "30000");*/
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (
                Cluster cassandraCluster = Cluster.builder().addContactPoint("127.0.0.1").build();
                Session cassandraSession = cassandraCluster.connect("ramp_up");
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties)) {
            // read stations and visitors data from Cassandra
            logger.info("stationUUIDs:");
            List<UUID> stationUUIDs = Lists.newArrayList();
            ResultSet rs = cassandraSession.execute("SELECT id FROM \"Station\"");
            for (Row row : rs) {
                UUID stationUUID = row.get("id", UUID.class);
                stationUUIDs.add(stationUUID);
                logger.info(stationUUID.toString());
            }
            logger.info("visitorUUIDs:");
            List<UUID> visitorUUIDs = Lists.newArrayList();
            rs = cassandraSession.execute("SELECT id FROM \"Visitor\"");
            for (Row row : rs) {
                UUID visitorUUID = row.get("id", UUID.class);
                visitorUUIDs.add(visitorUUID);
                logger.info(visitorUUID.toString());
            }
            // read visits from Kafka and put to Cassandra
            logger.info("read/write visits");
            consumer.subscribe(Arrays.asList(PutDataToKafka.KAFKA_TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (records.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    logger.info(String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()));
                    String[] parts = record.value().split(PutDataToKafka.DELIMITER);
                    UUID visitorId = UUID.fromString(parts[0]);
                    UUID stationId = UUID.fromString(parts[1]);
                    float amount = Float.parseFloat(parts[2]);
                    float cost = Float.parseFloat(parts[3]);
                    cassandraSession.execute("INSERT INTO \"Visit\" (id, \"visitorId\", \"stationId\", amount, cost) VALUES (?, ?, ?, ?, ?)",
                            UUID.fromString(record.key()), visitorId, stationId, amount, cost);
                }
            }
        }
        System.exit(0);
    }
}