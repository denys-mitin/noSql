package rampup.nosql.kafka;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rampup.nosql.cassandra.PutDataToCassandra;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class PutDataToKafka {

    private static Logger logger = LoggerFactory.getLogger(PutDataToKafka.class);

    public static final String KAFKA_TOPIC = "visits";

    public static final String DELIMITER = ":";

    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        /*kafkaProperties.put("acks", "all");
        kafkaProperties.put("retries", 0);
        kafkaProperties.put("batch.size", 16384);
        kafkaProperties.put("linger.ms", 1);
        kafkaProperties.put("buffer.memory", 33554432);*/
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (
                Cluster cassandraCluster = Cluster.builder().addContactPoint("127.0.0.1").build();
                Session cassandraSession = cassandraCluster.connect("ramp_up");
                Producer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {
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
            // put visits to Kafka
            logger.info("create visits");
            Random r = new Random(System.currentTimeMillis());
            for (int i = 0; i < PutDataToCassandra.VISITS_COUNT; i++) {
                String key = UUID.randomUUID().toString();
                UUID stationId = stationUUIDs.get(r.nextInt(PutDataToCassandra.STATIONS_COUNT));
                UUID visitorId = visitorUUIDs.get(r.nextInt(PutDataToCassandra.VISITORS_COUNT));
                float amount = r.nextFloat();
                float cost = r.nextFloat();
                String value = String.join(DELIMITER, visitorId.toString(), stationId.toString(), Float.toString(amount), Float.toString(cost));
                kafkaProducer.send(new ProducerRecord<>(KAFKA_TOPIC, key, value));
                logger.info(key + ", " + value);
            }
        }
        System.exit(0);
    }
}