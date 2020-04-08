package com.mqs.config;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

@Component
public class EsMethod {
    @Autowired
    RestHighLevelClient restHighLevelClient;
    public static Integer month = getNowMonth();

    public static boolean pdIndex(RestHighLevelClient restHighLevelClient, String indexName) {
        GetIndexRequest getRequest = new GetIndexRequest(indexName);
        try {
            boolean exists = restHighLevelClient.indices().exists(getRequest, RequestOptions.DEFAULT);
            return exists;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static Integer getNextMonth() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(calendar.MONTH, 1);
        return calendar.get(Calendar.MONTH) + 1;
    }

    public static Integer getNowMonth() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(calendar.MONTH, 1);
        return calendar.get(Calendar.MONTH);
    }

    public static Map<String, String> createMapper() {
        Map<String, String> map = new HashMap<>();
        map.put("id", "keyword"); //主键id
        map.put("price", "integer");//价格
        map.put("color", "keyword");//颜色
        map.put("sole", "date");//日期
        map.put("make", "keyword"); //生厂商
        return map;
    }

    public static void createIndex(RestHighLevelClient restHighLevelClient, Map<String, String> map, Integer month) {
        try {
            //创建索引对象
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("car_2020_" + month);
            Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    while (it.hasNext()) {
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
            createIndexRequest.mapping("emp", builder);

            //设置参数
            createIndexRequest.settings(Settings.builder().put("number_of_shards", "5").put("number_of_replicas", "1"));
            //指定映射
            //操作索引的客户端
            IndicesClient indices = restHighLevelClient.indices();
            //执行创建索引库
            CreateIndexResponse createIndexResponse = indices.create(createIndexRequest, RequestOptions.DEFAULT);
            //得到响应
            boolean acknowledged = createIndexResponse.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void jiancha() {
        new Timer("testTimer1").scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Integer nextMonth = getNextMonth();
                Integer nowMonth = getNowMonth();
                getNowMonth();
                boolean b = pdIndex(restHighLevelClient, "car_2020_" + nextMonth);
                boolean a = pdIndex(restHighLevelClient, "car_2020_" + nowMonth);
                if (!b) {
                    Map<String, String> mapper = createMapper();
                    createIndex(restHighLevelClient, mapper, nextMonth);
                    month = getNowMonth();
                }
                if (!a) {
                    Map<String, String> mapper = createMapper();
                    createIndex(restHighLevelClient, mapper, nowMonth);
                }
            }
        }, new Date(), 1000 * 60 * 24 * 30);
    }

}
