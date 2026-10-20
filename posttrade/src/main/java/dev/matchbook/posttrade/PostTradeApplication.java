package dev.matchbook.posttrade;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PostTradeApplication {

    public static void main(String[] args) {
        SpringApplication.run(PostTradeApplication.class, args);
    }
}
