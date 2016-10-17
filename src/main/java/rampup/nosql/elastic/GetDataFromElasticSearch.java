package rampup.nosql.elastic;

import com.google.gson.Gson;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rampup.nosql.Integration;
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

            // query for 1 record
            SearchResponse response = client.prepareSearch(Integration.ES_INDEX)
                    .setSize(1).execute().actionGet();
            for (SearchHit hit : response.getHits()) {
                logger.info(new Gson().fromJson(hit.getSourceAsString(), Visit.class).toString());
            }

            // Stations by total amount of petrol sold
            response = client.prepareSearch(Integration.ES_INDEX)
                    .addAggregation(AggregationBuilders.terms("by_sum_amount").field("station.name")
                            .order(Terms.Order.aggregation("sum_amount", false))
                            .subAggregation(AggregationBuilders.sum("sum_amount").field("amount")))
                    .setSize(0).execute().actionGet();
            for (Aggregation aggregation : response.getAggregations()) {
                logger.info(aggregation.toString());
            }
        }
        System.exit(0);
    }
}