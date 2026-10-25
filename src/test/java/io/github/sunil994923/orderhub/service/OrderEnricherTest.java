package io.github.sunil994923.orderhub.service;

import io.github.sunil994923.orderhub.OrderHubProperties;
import io.github.sunil994923.orderhub.model.OrderEvent;
import io.github.sunil994923.orderhub.model.OrderItem;
import io.github.sunil994923.orderhub.model.ShippingMethod;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderEnricherTest {

    private final OrderEnricher enricher = new OrderEnricher(
            new OrderHubProperties(null, new BigDecimal("1000.00"), null));

    @Test
    void calculatesTotalAcrossLines() {
        OrderEvent enriched = enricher.enrich(order(
                new OrderItem("SKU-1", 2, new BigDecimal("19.99")),
                new OrderItem("SKU-2", 1, new BigDecimal("5.005"))));

        assertThat(enriched.totalAmount()).isEqualByComparingTo("44.99");
        assertThat(enriched.highValue()).isFalse();
    }

    @Test
    void flagsOrdersAtOrAboveThresholdAsHighValue() {
        OrderEvent enriched = enricher.enrich(order(new OrderItem("SKU-1", 4, new BigDecimal("250.00"))));

        assertThat(enriched.totalAmount()).isEqualByComparingTo("1000.00");
        assertThat(enriched.highValue()).isTrue();
    }

    @Test
    void rejectsOrderWithoutItems() {
        assertThatThrownBy(() -> enricher.enrich(order()))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("no items");
    }

    @Test
    void rejectsNonPositivePrice() {
        assertThatThrownBy(() -> enricher.enrich(order(new OrderItem("SKU-1", 1, BigDecimal.ZERO))))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("SKU-1");
    }

    private static OrderEvent order(OrderItem... items) {
        return new OrderEvent("order-1", Instant.parse("2026-01-01T00:00:00Z"), "customer-1",
                List.of(items), ShippingMethod.STANDARD, null, null);
    }
}
