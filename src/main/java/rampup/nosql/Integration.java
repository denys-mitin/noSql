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
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.joda.time.DateTimeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rampup.nosql.data.Station;
import rampup.nosql.data.Visit;
import rampup.nosql.data.Visitor;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class Integration {

    private static Logger logger = LoggerFactory.getLogger(Integration.class);

    public static final int RANDOM_HOURS = 100;

    public static final String CASSANDRA_KEYSPACE = "ramp_up";

    public static final String ES_INDEX = "ramp_up";

    public static final String ES_TYPE = "visit";

    private static final String ES_MAPPING =
            "{" +
                "\"properties\": {" +
                "  \"station\": {" +
                "    \"properties\": {" +
                "      \"location\": {" +
                "        \"type\": \"geo_point\"" +
                "      }" +
                "    }" +
                "  }," +
                "  \"time\": {" +
                "    \"type\": \"date\"" +
                "  }" +
                "}" +
            "}";

    public static void main(String[] args) {
        try (
                Cluster cassandraCluster = Cluster.builder().addContactPoint("127.0.0.1").build();
                Session cassandraSession = cassandraCluster.connect(CASSANDRA_KEYSPACE);
                TransportClient elasticClient = TransportClient.builder().build()
                        .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)))) {

            // drop ElasticSearch index
            IndicesExistsRequest existsRequest = elasticClient.admin().indices().prepareExists(ES_INDEX).request();
            if (elasticClient.admin().indices().exists(existsRequest).actionGet().isExists()) {
                logger.info(String.format("index %s exists... deleting!", ES_INDEX));
                DeleteIndexResponse deleteResponse = elasticClient.admin().indices().delete(new DeleteIndexRequest(ES_INDEX)).actionGet();
                if (!deleteResponse.isAcknowledged()) {
                    logger.error(String.format("Failed to delete ElasticSearch index named %s", ES_INDEX));
                }
            }
            if (elasticClient.admin().indices().exists(existsRequest).actionGet().isExists()) {
                logger.error(String.format("Index %s still exists", ES_INDEX));
                System.exit(1);
            }

            // create/update ElasticSearch template
            PutIndexTemplateRequestBuilder indexTemplateRequestBuilder = elasticClient.admin().indices()
                    .preparePutTemplate(ES_INDEX).addMapping(ES_TYPE, ES_MAPPING).setTemplate(ES_INDEX);
            PutIndexTemplateResponse templateResponse = indexTemplateRequestBuilder.execute().actionGet();
            if (!templateResponse.isAcknowledged()) {
                logger.error(String.format("Failed to create/update ElasticSearch template named %s", ES_INDEX));
            }

            // fetch from Cassandra in batches and put to ElasticSearch
            Map<UUID, Station> stations = Maps.newHashMap();
            ResultSet rs = cassandraSession.execute("SELECT * FROM \"Station\"");
            Random r = new Random(System.currentTimeMillis());
            for (Row row : rs) {
                Station station = new Station();
                station.setId(row.get("id", UUID.class));
                station.setName(row.get("name", String.class));
                // let's generate random location
                // Ukraine's latitude is 49° N and longitude is 32° E
                GeoPoint location = new GeoPoint();
                location.resetLat(49 + r.nextInt(3)).resetLon(32 + r.nextInt(3));
                station.setLocation(location);
                stations.put(station.getId(), station);
                logger.info(station.toString());
            }
            Map<UUID, Visitor> visitors = Maps.newHashMap();
            rs = cassandraSession.execute("SELECT * FROM \"Visitor\"");
            for (Row row : rs) {
                Visitor visitor = new Visitor();
                visitor.setId(row.get("id", UUID.class));
                visitor.setFirstName(row.get("firstName", String.class));
                visitor.setLastName(row.get("lastName", String.class));
                visitors.put(visitor.getId(), visitor);
                logger.info(visitor.toString());
            }
            rs = cassandraSession.execute("SELECT * FROM \"Visit\"");
            for (Row row : rs) {
                Visit visit = new Visit();
                String id = row.get("id", UUID.class).toString().replace("-", "");
                visit.setStation(stations.get(row.get("stationId", UUID.class)));
                visit.setVisitor(visitors.get(row.get("visitorId", UUID.class)));
                visit.setAmount(row.get("amount", Float.class));
                visit.setCost(row.get("cost", Float.class));
                // let's generate random time
                visit.setTime(new Date().getTime() + r.nextInt(RANDOM_HOURS) * DateTimeConstants.MILLIS_PER_HOUR);
                logger.info(visit.toString());
                // put to ElasticSearch
                IndexRequest indexRequest = new IndexRequest(ES_INDEX, ES_TYPE, id);
                indexRequest.source(new Gson().toJson(visit));
                IndexResponse indexResponse = elasticClient.index(indexRequest).actionGet();
                logger.info((indexResponse.isCreated() ? "created: " : "failed to index: ") + id);
                break;
            }
        }
        System.exit(0);
    }
}
