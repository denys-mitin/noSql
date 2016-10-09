package rampup.nosql.elastic;

import com.google.gson.Gson;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rampup.nosql.Integration;
import rampup.nosql.cassandra.PutDataToCassandra;
import rampup.nosql.data.Visit;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

public class GetDataFromElasticSearch {

    private static Logger logger = LoggerFactory.getLogger(GetDataFromElasticSearch.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (
                TransportClient client = TransportClient.builder().build()
                        .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)))
               ) {
            SearchResponse response = client.prepareSearch(Integration.ES_INDEX).
                    setSize(PutDataToCassandra.VISITS_COUNT).execute().actionGet();
            for (SearchHit hit : response.getHits()) {
                logger.info(new Gson().fromJson(hit.getSourceAsString(), Visit.class).toString());
            }
        }
        System.exit(0);
    }
}