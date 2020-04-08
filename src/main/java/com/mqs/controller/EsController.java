package com.mqs.controller;

import com.mqs.Service.EsService;
import com.mqs.config.EsMethod;
import com.mqs.entity.Car;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 对es7.3.2进项的增删改查操作
 * @author mqs
 * @Time 2020-4-3
 */
@RestController
@RequestMapping("es")
public class EsController {
    @Autowired
    EsService esService;

    /**
     * 根据规则向索引中插入数据
     * @param car
     * @return  判断插入是否成功
     */
    @PostMapping("add")
    public Boolean add(@RequestBody Car car){
        System.out.println(car);
        boolean add = esService.add(car);
        return add;
    }

    /**
     * 根据id查询数据
     * @param id
     * @return
     */
    @GetMapping("queryById")
    public Car queryById(String id){
        Car car = esService.queryById(id);
        return car;
    }

    /**
     * 查寻所有
     * @return
     */
    @GetMapping("queryAll")
    public List<Car> queryAll(){
        List<Car> cars = esService.selectAll("car_2020_" + EsMethod.month, "emp");
        return cars;
    }
    @PostMapping("update")
    public boolean update(Car car){
        boolean update = esService.update(car);
        return update;
    }

    /**
     * 根据id删除
     * @param id
     * @return
     */
    @DeleteMapping("delete")
    public boolean delete(String id){
        boolean delete = esService.delete(id);
        return delete;
    }

    /**
     *根据传入集合id查询数据
     * @param ids
     * @return
     */
    @GetMapping("queryByIds")
    public List<Car> queryByIds(List<String> ids){
        List<Car> cars = esService.queryByIds(ids);
        return cars;
    }

    /**
     * 查询根据输入的字段进行分组
     * @param filed
     * @return
     */
    @GetMapping("queryTermBy")
    public Map<String,Long>queryTermBy(String filed){
        Map<String, Long> term = esService.term(filed);
        return term;
    }

    /**
     * 对第一次分组再分组
     * @param filed1
     * @param filed2
     * @return
     */
    @GetMapping("queryTermByTerm")
    public Map<String,Map<String,Long>>queryTermByTerm(String filed1,String filed2){
        Map<String, Map<String, Long>> stringMapMap = esService.termNextTerm(filed1, filed2);
        return stringMapMap;
    }

    @GetMapping("maxByPrice")
    public double maxByPrice(){
        double v = esService.maxByPrice();
        return v;
    }

}
