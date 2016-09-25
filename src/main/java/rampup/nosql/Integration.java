package rampup.nosql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;

public class Integration {

    public static void main(String[] args) {
        try (
                Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
                Session session = cluster.connect("ramp_up");
                TransportClient client = TransportClient.builder().build()
                        .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)))) {
            // get last use timestamp from ElasticSearch
            // fetch from Cassandra in batches and put to ElasticSearch
        }
        System.exit(0);
    }
}
