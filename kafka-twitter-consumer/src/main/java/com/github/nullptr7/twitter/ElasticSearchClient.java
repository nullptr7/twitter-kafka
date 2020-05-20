package com.github.nullptr7.twitter;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

public class ElasticSearchClient {

    final static RestHighLevelClient client;
    Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);


    static {
        client = createClient();
    }

    public static RestHighLevelClient createClient() {

        final String hostname = "kafka-twitter-course-3352605846.ap-southeast-2.bonsaisearch.net";
        final String username = "";
        final String password = "";

        final CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        final Function<CredentialsProvider, RestClientBuilder.HttpClientConfigCallback> configCallbackFunction =
                credProvider -> clientBuilder -> clientBuilder.setDefaultCredentialsProvider(credProvider);

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                                              .setHttpClientConfigCallback(configCallbackFunction.apply(provider));

        return new RestHighLevelClient(builder);
    }

    public void submitToElasticSearch(String tweets) {
        try {
            IndexRequest request = new IndexRequest("twitter", "tweet").source(tweets, XContentType.JSON);
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            logger.info("Id received is - {}", response.getId());

        } catch (Exception exception) {
            logger.error("Exception in connecting to ELK {}", exception.getMessage());
        }
    }

    public void closeClient() {
        try {
            client.close();
        } catch (IOException e) {
            logger.error("Issue closing client");
        }
    }

    public static void main(String[] args) {
        new ElasticSearchClient().submitToElasticSearch("{\n" +
                                                                "  \"foo\": \"boo\"\n" +
                                                                "}");
    }
}
