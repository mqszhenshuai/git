package com.mqs;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.mqs.mapper")
public class SpringDataEsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringDataEsApplication.class, args);
    }

}
