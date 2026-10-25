package io.github.sunil994923.orderhub.service;

import io.github.sunil994923.orderhub.model.OrderEvent;
import io.github.sunil994923.orderhub.model.OrderRequest;
import io.github.sunil994923.orderhub.route.OrderRoutes;
import org.apache.camel.ProducerTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

@Service
public class OrderSubmissionService {

    private final ProducerTemplate producer;
    private final Clock clock;

    public OrderSubmissionService(ProducerTemplate producer, Clock clock) {
        this.producer = producer;
        this.clock = clock;
    }

    /** Publishes the order to the inbound topic and returns the generated order id. */
    public String submit(OrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        producer.sendBody(OrderRoutes.INTAKE, OrderEvent.from(orderId, Instant.now(clock), request));
        return orderId;
    }
}
