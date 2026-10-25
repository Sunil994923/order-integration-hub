package io.github.sunil994923.orderhub.route;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.sunil994923.orderhub.OrderHubProperties;
import io.github.sunil994923.orderhub.model.OrderEvent;
import io.github.sunil994923.orderhub.service.InvalidOrderException;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.support.processor.idempotent.MemoryIdempotentRepository;
import org.springframework.stereotype.Component;

/**
 * Integration flow:
 * <pre>
 * REST -> direct:order-intake -> [orders.inbound]
 * [orders.inbound] -> idempotent consumer -> enrich -> content-based router
 *     EXPRESS  -> [orders.express]
 *     STANDARD -> [orders.standard]
 *     high value (wire tap) -> [orders.review]
 * failures -> retry with back-off -> [orders.dlq]
 * </pre>
 */
@Component
public class OrderRoutes extends RouteBuilder {

    public static final String INTAKE = "direct:order-intake";
    public static final String ROUTE_INTAKE = "order-intake";
    public static final String ROUTE_PROCESSOR = "order-processor";
    public static final String ROUTE_REVIEW_TAP = "high-value-review-tap";
    static final String HIGH_VALUE_TAP = "direct:high-value-tap";

    // Header names avoid dots: Camel's header() expression treats a dot as an OGNL path.
    public static final String ORDER_ID_HEADER = "orderhub-order-id";
    public static final String ERROR_HEADER = "orderhub-error";
    public static final String SHIPPING_HEADER = "orderhub-shipping-method";
    public static final String HIGH_VALUE_HEADER = "orderhub-high-value";

    private final OrderHubProperties properties;

    public OrderRoutes(OrderHubProperties properties) {
        this.properties = properties;
    }

    @Override
    public void configure() {
        OrderHubProperties.Endpoints endpoints = properties.endpoints();
        OrderHubProperties.Redelivery redelivery = properties.redelivery();
        JacksonDataFormat json = orderJson();

        // Transient failures (e.g. broker unavailable) are retried with exponential back-off,
        // then parked on the dead-letter topic with the original payload.
        errorHandler(deadLetterChannel(endpoints.deadLetter())
                .useOriginalMessage()
                .maximumRedeliveries(redelivery.maximumRedeliveries())
                .redeliveryDelay(redelivery.redeliveryDelayMs())
                .backOffMultiplier(redelivery.backOffMultiplier())
                .useExponentialBackOff()
                .retryAttemptedLogLevel(LoggingLevel.WARN));

        // Retrying cannot fix a malformed order, so these skip redelivery.
        onException(InvalidOrderException.class, JsonProcessingException.class)
                .maximumRedeliveries(0)
                .handled(true)
                .useOriginalMessage()
                .setHeader(ERROR_HEADER, exceptionMessage())
                .log(LoggingLevel.ERROR, "Rejected order: ${exception.message}")
                .to(endpoints.deadLetter());

        from(INTAKE).routeId(ROUTE_INTAKE)
                .process(OrderRoutes::setOrderKey)
                .marshal(json)
                .to(endpoints.inbound())
                .log("Published order ${header." + ORDER_ID_HEADER + "} to inbound topic");

        from(endpoints.inboundConsumer()).routeId(ROUTE_PROCESSOR)
                .unmarshal(json)
                .process(OrderRoutes::setOrderKey)
                .idempotentConsumer(header(ORDER_ID_HEADER),
                        MemoryIdempotentRepository.memoryIdempotentRepository(10_000))
                .bean("orderEnricher", "enrich")
                .process(OrderRoutes::setRoutingHeaders)
                .marshal(json)
                .wireTap(HIGH_VALUE_TAP)
                .choice()
                    .when(header(SHIPPING_HEADER).isEqualTo("EXPRESS"))
                        .to(endpoints.express())
                    .otherwise()
                        .to(endpoints.standard())
                .end()
                .log("Routed order ${header." + ORDER_ID_HEADER + "} as ${header." + SHIPPING_HEADER + "}");

        // Runs on a copy of the exchange, so a slow or failing review topic never blocks fulfilment.
        from(HIGH_VALUE_TAP).routeId(ROUTE_REVIEW_TAP)
                .filter(header(HIGH_VALUE_HEADER).isEqualTo(true))
                .to(endpoints.review())
                .log("Order ${header." + ORDER_ID_HEADER + "} flagged for review");
    }

    /** Keys Kafka messages by order id so all events for one order land on the same partition. */
    private static void setOrderKey(Exchange exchange) {
        OrderEvent order = exchange.getMessage().getBody(OrderEvent.class);
        if (order.orderId() == null || order.orderId().isBlank()) {
            throw new InvalidOrderException("Order has no orderId");
        }
        exchange.getMessage().setHeader(KafkaConstants.KEY, order.orderId());
        exchange.getMessage().setHeader(ORDER_ID_HEADER, order.orderId());
    }

    /** Routing metadata travels as Kafka headers so downstream consumers can filter without parsing. */
    private static void setRoutingHeaders(Exchange exchange) {
        OrderEvent order = exchange.getMessage().getBody(OrderEvent.class);
        exchange.getMessage().setHeader(SHIPPING_HEADER, order.shippingMethod().name());
        exchange.getMessage().setHeader(HIGH_VALUE_HEADER, order.highValue());
    }

    private static JacksonDataFormat orderJson() {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JacksonDataFormat format = new JacksonDataFormat(mapper, OrderEvent.class);
        format.setAutoDiscoverObjectMapper(false);
        return format;
    }
}
