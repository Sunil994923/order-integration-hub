package io.github.sunil994923.orderhub.route;

import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit6.CamelSpringBootTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@CamelSpringBootTest
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class OrderRoutesTest {

    @Autowired
    private ProducerTemplate producer;

    @EndpointInject("mock:standard")
    private MockEndpoint standard;

    @EndpointInject("mock:express")
    private MockEndpoint express;

    @EndpointInject("mock:review")
    private MockEndpoint review;

    @EndpointInject("mock:dlq")
    private MockEndpoint dlq;

    @AfterEach
    void reset() {
        MockEndpoint.resetMocks(standard.getCamelContext());
    }

    @Test
    void routesStandardOrder() throws Exception {
        standard.expectedMessageCount(1);
        standard.expectedHeaderReceived(KafkaConstants.KEY, "o-1");
        standard.expectedHeaderReceived(OrderRoutes.SHIPPING_HEADER, "STANDARD");
        express.expectedMessageCount(0);
        review.expectedMessageCount(0);

        send(order("o-1", "STANDARD", 2, "10.00"));

        MockEndpoint.assertIsSatisfied(standard, express, review);
        String body = standard.getReceivedExchanges().getFirst().getIn().getBody(String.class);
        assertThat(body).contains("\"totalAmount\":20.00").contains("\"highValue\":false");
    }

    @Test
    void routesExpressOrder() throws Exception {
        express.expectedMessageCount(1);
        standard.expectedMessageCount(0);

        send(order("o-2", "EXPRESS", 1, "15.00"));

        MockEndpoint.assertIsSatisfied(standard, express);
    }

    @Test
    void wireTapsHighValueOrderToReview() throws Exception {
        review.expectedMessageCount(1);
        review.expectedHeaderReceived(OrderRoutes.HIGH_VALUE_HEADER, true);
        standard.expectedMessageCount(1);

        send(order("o-3", "STANDARD", 5, "300.00"));

        MockEndpoint.assertIsSatisfied(review, standard);
    }

    @Test
    void dropsDuplicateOrderIds() throws Exception {
        standard.expectedMessageCount(1);

        send(order("o-4", "STANDARD", 1, "1.00"));
        send(order("o-4", "STANDARD", 1, "1.00"));

        MockEndpoint.assertIsSatisfied(standard);
    }

    @Test
    void sendsInvalidOrderToDeadLetterWithoutRetry() throws Exception {
        dlq.expectedMessageCount(1);
        dlq.message(0).header(OrderRoutes.ERROR_HEADER).contains("SKU-1");
        standard.expectedMessageCount(0);

        send(order("o-5", "STANDARD", 1, "0"));

        MockEndpoint.assertIsSatisfied(dlq, standard);
    }

    @Test
    void sendsMalformedJsonToDeadLetter() throws Exception {
        dlq.expectedBodiesReceived("not-json");

        send("not-json");

        MockEndpoint.assertIsSatisfied(dlq);
    }

    private void send(String json) {
        producer.sendBody("direct:inbound", json);
    }

    private static String order(String id, String shipping, int quantity, String unitPrice) {
        return """
                {"orderId":"%s","receivedAt":"2026-01-01T00:00:00Z","customerId":"c-1",
                 "items":[{"sku":"SKU-1","quantity":%d,"unitPrice":%s}],"shippingMethod":"%s"}
                """.formatted(id, quantity, unitPrice, shipping);
    }
}
