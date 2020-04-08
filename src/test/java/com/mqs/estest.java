package com.mqs;


import com.mqs.config.EsMethod;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;


@SpringBootTest(classes = SpringDataEsApplication.class)
@RunWith(SpringRunner.class)
public class estest {

    /**
     * @param
     * @return
     * @author:刘鹤翔
     * @description:获取当前月的下一个月份
     * @date:2019/11/5 10:09
     * @exception/throws 无
     */
  @Autowired
  RestHighLevelClient restHighLevelClient;
    @Test
    public void test1() {
        GetRequest getRequest = new GetRequest("dangdang");
        System.out.println(getRequest);
        if (getRequest == null) {
            System.out.println(false);
        } else
            System.out.println(true);

    }

    @Test
    public void testCreateIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("car");
        Map<String,String> map = new HashMap<>();
        map.put("id","keyword"); //主键id
        map.put("price","integer");//每条数据的唯一标识
        map.put("name","text");//卡口id
        map.put("sole","date");//经过地点编号
        map.put("make","text"); //设备编号，第三方设备编号
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                while (it.hasNext()){
                    Map.Entry<String, String> entry = it.next();
                    builder.startObject(entry.getKey());
                    {
                        builder.field("type", entry.getValue());
                    }
                    builder.endObject();
                }
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping("emp", builder);
        CreateIndexResponse createIndexResponse =restHighLevelClient.indices().create(request,RequestOptions.DEFAULT);
        System.out.println("-----------createIndexResponse-----------" + createIndexResponse.toString());
    }
    @Test
    public void time() throws UnknownHostException {
        SearchRequest searchRequest = new SearchRequest("car");
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0);
                searchSourceBuilder.query(QueryBuilders.matchAllQuery());
                TermsAggregationBuilder builder = AggregationBuilders.terms("colors").field("color");
                TermsAggregationBuilder sum = AggregationBuilders.terms("make").field("color");
                searchSourceBuilder.aggregation(builder.subAggregation(sum));
                SearchRequest source = searchRequest.source(searchSourceBuilder);
                try {
                    restHighLevelClient.search(source,RequestOptions.DEFAULT);
                } catch (IOException e) {
                    e.printStackTrace();
                }

    }
    @Test
    public void highClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.136.6", 9200, "http")));
        System.out.println(client.toString());
    }
    @Test
    public void trem() throws IOException {
        Map<String, Map<String,Long>> groupMap = new HashMap<>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(100);
        AggregationBuilder agg1 = AggregationBuilders.terms("agg").field("color");
        AggregationBuilder agg2 = AggregationBuilders.max("agg2").field("agg").subAggregation(agg1);
        searchSourceBuilder.aggregation(agg2);
        SearchRequest searchRequest = new SearchRequest("car_2020_"+ EsMethod.month).types("emp");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println(searchResponse);
            Terms terms = searchResponse.getAggregations().get("agg");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

