package com.mqs.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.UnknownHostException;

@Configuration
public class ESConfig {
    @Value(value = "${spring.data.elasticsearch.cluster-nodes}")
    private String host;
    @Bean
    public RestHighLevelClient client() throws UnknownHostException {
        String[] split = host.split(":");
        HttpHost httpHost=new HttpHost(split[0],Integer.parseInt(split[1]));
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(split[0], Integer.parseInt(split[1]), "http")));
        return client;
    }
}
