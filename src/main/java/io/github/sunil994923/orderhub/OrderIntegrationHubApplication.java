package io.github.sunil994923.orderhub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class OrderIntegrationHubApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderIntegrationHubApplication.class, args);
    }
}
