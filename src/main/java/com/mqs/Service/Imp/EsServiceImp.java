package com.mqs.Service.Imp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mqs.Service.EsService;
import com.mqs.config.EsMethod;
import com.mqs.entity.Car;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class EsServiceImp implements EsService {
    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Override
    public boolean add(Car car) {
        //文档内容
        //准备json数据
        try {

            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("id", car.getId());
            jsonMap.put("color", car.getColor());
            jsonMap.put("make", car.getMake());
            jsonMap.put("price", car.getPrice());
            jsonMap.put("sold", car.getSold());
            //创建索引创建对象
            IndexRequest indexRequest = new IndexRequest("car_2020_"+ EsMethod.month, "emp").source(jsonMap);
            //通过client进行http的请求
            IndexResponse indexResponse = null;

            indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            DocWriteResponse.Result result = indexResponse.getResult();
            System.out.println(result);
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Car queryById(String id) {
        try {
            Car car = new Car();
            GetRequest getRequest = new GetRequest("car_2020_"+EsMethod.month, "emp", id);
            GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            String sold = (String) documentFields.getSource().get("sold");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

            Date parse = simpleDateFormat.parse(sold);

            Integer price = (Integer) documentFields.getSource().get("price");
            String color = (String) documentFields.getSource().get("color");
            String make = (String) documentFields.getSource().get("make");
            car.setColor(color).setMake(make).setPrice(price).setSold(parse);
            return car;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Car> selectAll(String indexName,String type){
        List<Car> cars = new ArrayList<>();
        try{
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
            SearchRequest searchRequest = new SearchRequest(indexName).types(type); //索引
            searchSourceBuilder.query(queryBuilder);
            searchRequest.source(searchSourceBuilder);
            SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();
            ObjectMapper mapper = new ObjectMapper();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                Car car = mapper.readValue(sourceAsString, Car.class);
                cars.add(car);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cars;
    }
    @Override
    public boolean delete(String id) {
        boolean a=false;
        DeleteRequest request = new DeleteRequest("car_2020_" + EsMethod.month, "emp", id);
        try {
            DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
            System.out.println(delete);
            a=true;
        } catch (IOException e) {
            a=false;
            e.printStackTrace();
        }
        return a;
    }

    @Override
    public boolean update(Car car) {
        boolean a=false;
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id", car.getId());
        jsonMap.put("color", car.getColor());
        jsonMap.put("make", car.getMake());
        jsonMap.put("price", car.getPrice());
        jsonMap.put("sold", car.getSold());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        UpdateRequest request = new UpdateRequest("car_2020_" + EsMethod.month, car.getId());
        try {
            UpdateResponse update = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            a=true;
        } catch (IOException e) {
            a=false;
            e.printStackTrace();
        }
        return a;
    }
    public List<Car> queryByIds(List<String> ids)  {
        List<Car>cars=new ArrayList<>();
        try {
        MultiGetRequest request = new MultiGetRequest();
        ids.stream().forEach(id -> {
            request.add(new MultiGetRequest.Item("car_2020_4","emp", id));
        });
        MultiGetResponse response = restHighLevelClient.mget(request, RequestOptions.DEFAULT);
        GetResponse getResponse;
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < response.getResponses().length; i++) {
            getResponse = response.getResponses()[i].getResponse();
            if (getResponse.isExists()) {
                Car car = new Car();
                car = mapper.readValue(getResponse.getSourceAsString(), Car.class);
                cars.add(car);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cars;
    }

    @Override
    public Map<String, Long> term(String filed) {
        Map<String, Long> groupMap = new HashMap<>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(100);
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("agg").field(filed);
        searchSourceBuilder.aggregation(aggregationBuilder);

        SearchRequest searchRequest = new SearchRequest("car_2020_"+EsMethod.month).types("emp");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Terms terms = searchResponse.getAggregations().get("agg");
            for (Terms.Bucket entry : terms.getBuckets()) {
                groupMap.put(entry.getKey().toString(), entry.getDocCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return groupMap;
    }

    @Override
    public Map<String, Map<String, Long>> termNextTerm(String filed1, String filed2) {
        Map<String, Map<String,Long>> groupMap = new HashMap<>();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(100);
        AggregationBuilder agg1 = AggregationBuilders.terms("agg").field(filed1);
        AggregationBuilder agg2 = AggregationBuilders.terms("agg2").field(filed2);
        agg1.subAggregation(agg2);
        searchSourceBuilder.aggregation(agg1);
        SearchRequest searchRequest = new SearchRequest("car_2020_"+EsMethod.month).types("emp");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Terms terms = searchResponse.getAggregations().get("agg");
            Terms terms2;
            for (Terms.Bucket bucket1 : terms.getBuckets()) {
                terms2 = bucket1.getAggregations().get("agg2");
                Map<String, Long> map2 = new HashMap<>();
                for (Terms.Bucket bucket2 : terms2.getBuckets()) {
                    map2.put(bucket2.getKey().toString(), bucket2.getDocCount());
                }
                groupMap.put(bucket1.getKey().toString(), map2);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return groupMap;
    }
    public double maxByPrice(){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);

        AggregationBuilder aggregationBuilder = AggregationBuilders.max("agg").field("price");
        searchSourceBuilder.aggregation(aggregationBuilder);

        SearchRequest searchRequest = new SearchRequest("car_2020_4").types("emp");
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        double value=0.0;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            Max agg = searchResponse.getAggregations().get("agg");
             value= agg.getValue();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;

    }
}
