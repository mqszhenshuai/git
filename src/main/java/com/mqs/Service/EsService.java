package com.mqs.Service;

import com.mqs.entity.Car;

import java.util.List;
import java.util.Map;

public interface EsService {
    boolean add(Car car);
    Car queryById(String id);
    boolean delete(String id);
    boolean update(Car car);
    List<Car> selectAll(String indexName,String type);
    List<Car> queryByIds(List<String> ids);
    Map<String ,Long>term(String filed);
    Map<String,Map<String,Long>>termNextTerm(String filed1,String filed2);
    double maxByPrice();

}