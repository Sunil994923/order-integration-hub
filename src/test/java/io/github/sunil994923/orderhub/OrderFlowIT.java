package io.github.sunil994923.orderhub;

import io.github.sunil994923.orderhub.model.OrderItem;
import io.github.sunil994923.orderhub.model.OrderRequest;
import io.github.sunil994923.orderhub.model.ShippingMethod;
import io.github.sunil994923.orderhub.service.OrderSubmissionService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end flow against an in-JVM Kafka broker: service -> inbound topic -> Camel router -> output topics. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "camel.component.kafka.brokers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1,
        topics = {"orders.inbound", "orders.standard", "orders.express", "orders.review", "orders.dlq"})
class OrderFlowIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private OrderSubmissionService submissionService;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    void highValueExpressOrderReachesExpressAndReviewTopics() {
        try (Consumer<String, String> consumer = consumer()) {
            broker.consumeFromEmbeddedTopics(consumer, "orders.express", "orders.review");

            String orderId = submissionService.submit(new OrderRequest("customer-42",
                    List.of(new OrderItem("LAPTOP-15", 1, new BigDecimal("1499.00"))),
                    ShippingMethod.EXPRESS));

            List<ConsumerRecord<String, String>> records = poll(consumer, 2);

            assertThat(records).extracting(ConsumerRecord::topic)
                    .containsExactlyInAnyOrder("orders.express", "orders.review");
            assertThat(records).allSatisfy(record -> {
                assertThat(record.key()).isEqualTo(orderId);
                assertThat(record.value()).contains("\"totalAmount\":1499.00", "\"highValue\":true");
            });
        }
    }

    private List<ConsumerRecord<String, String>> poll(Consumer<String, String> consumer, int expected) {
        List<ConsumerRecord<String, String>> received = new java.util.ArrayList<>();
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (received.size() < expected && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(500)).forEach(received::add);
        }
        return received;
    }

    private Consumer<String, String> consumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "order-flow-it", false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }
}
