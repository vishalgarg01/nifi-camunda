package com.emigran;

import org.camunda.bpm.spring.boot.starter.annotation.EnableProcessApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.emigran")
@EnableProcessApplication
public class CamundaNifiTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(CamundaNifiTestApplication.class, args);
    }
}
