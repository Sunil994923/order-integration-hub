package io.github.sunil994923.orderhub;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

/**
 * Endpoint URIs are externalised so the same routes run against Kafka in production
 * and against in-memory endpoints in tests.
 */
@ConfigurationProperties(prefix = "orderhub")
public record OrderHubProperties(
        Endpoints endpoints,
        BigDecimal highValueThreshold,
        Redelivery redelivery) {

    public record Endpoints(
            String inbound,
            String inboundConsumer,
            String standard,
            String express,
            String review,
            String deadLetter) {
    }

    public record Redelivery(int maximumRedeliveries, long redeliveryDelayMs, double backOffMultiplier) {
    }
}
