package rampup.nosql.elastic;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class GetDataFromElasticSearch {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (
                TransportClient client = TransportClient.builder().build()
                        .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)))
               ) {
            ActionFuture<GetResponse> response = client.get(new GetRequest("visit").id(UUID.randomUUID().toString()));
            if (response.isDone()) {
                System.out.println(response.get().toString());
            }
            response = client.get(new GetRequest("visit").id("d7ebb2fb-09f5-4741-9207-ebe22b304771"));
            if (response.isDone()) {
                System.out.println(response.get().toString());
            }
            client.close();
        }
        System.exit(0);
    }
}