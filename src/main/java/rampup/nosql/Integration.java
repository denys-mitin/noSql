package rampup.nosql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rampup.nosql.data.Station;
import rampup.nosql.data.Visit;
import rampup.nosql.data.Visitor;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;

public class Integration {

    private static Logger logger = LoggerFactory.getLogger(Integration.class);

    public static final String INDEX = "ramp_up";

    public static final String TYPE = "visit";

    private static Map<UUID, Station> stations = Maps.newHashMap();

    private static Map<UUID, Visitor> visitors = Maps.newHashMap();

    public static void main(String[] args) {
        try (
                Cluster cassandraCluster = Cluster.builder().addContactPoint("127.0.0.1").build();
                Session cassandraCession = cassandraCluster.connect("ramp_up");
                TransportClient elasticClient = TransportClient.builder().build()
                        .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)))) {
            // drop ElasticSearch index
            IndicesExistsRequest existsRequest = elasticClient.admin().indices().prepareExists(INDEX).request();
            if (elasticClient.admin().indices().exists(existsRequest).actionGet().isExists()) {
                logger.info(String.format("index %s exists... deleting!", INDEX));
                DeleteIndexResponse response = elasticClient.admin().indices().delete(new DeleteIndexRequest(INDEX)).actionGet();
                if (!response.isAcknowledged()) {
                    logger.error(String.format("Failed to delete elastic search index named %s", INDEX));
                }
            }
            if (elasticClient.admin().indices().exists(existsRequest).actionGet().isExists()) {
                logger.error(String.format("Index %s still exists", INDEX));
                System.exit(1);
            }
            // fetch from Cassandra in batches and put to ElasticSearch
            ResultSet rs = cassandraCession.execute("SELECT * FROM \"Station\"");
            for (Row row : rs) {
                Station station = new Station();
                station.setId(row.get("id", UUID.class));
                station.setName(row.get("name", String.class));
                stations.put(station.getId(), station);
                logger.info(station.toString());
            }
            rs = cassandraCession.execute("SELECT * FROM \"Visitor\"");
            for (Row row : rs) {
                Visitor visitor = new Visitor();
                visitor.setId(row.get("id", UUID.class));
                visitor.setFirstName(row.get("firstName", String.class));
                visitor.setLastName(row.get("lastName", String.class));
                visitors.put(visitor.getId(), visitor);
                logger.info(visitor.toString());
            }
            rs = cassandraCession.execute("SELECT * FROM \"Visit\"");
            for (Row row : rs) {
                Visit visit = new Visit();
                visit.setId(row.get("id", UUID.class));
                visit.setStation(stations.get(row.get("stationId", UUID.class)));
                visit.setVisitor(visitors.get(row.get("visitorId", UUID.class)));
                visit.setAmount(row.get("amount", Float.class));
                visit.setCost(row.get("cost", Float.class));
                logger.info(visit.toString());
                // put to ElasticSearch
                IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, visit.getId().toString());
                indexRequest.source(new Gson().toJson(visit));
                IndexResponse response = elasticClient.index(indexRequest).actionGet();
                logger.info((response.isCreated() ? "created: " : "failed to index: ") + visit.getId());
            }
        }
        System.exit(0);
    }
}
