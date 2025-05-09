package com.example.pocconcurrency;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@EnableScheduling
@Configuration
public class AppConfig {

    public @Bean MongoClient mongoClient() {
        return MongoClients.create("mongodb://root:example@localhost:27017");
    }

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5); // Allow concurrent execution
        scheduler.setThreadNamePrefix("worker-thread-");
        scheduler.initialize();
        return scheduler;
    }
}
