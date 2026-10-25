package io.github.sunil994923.orderhub.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Canonical order message published to Kafka. {@code totalAmount} and {@code highValue}
 * are null until the enrichment step has run.
 */
public record OrderEvent(
        String orderId,
        Instant receivedAt,
        String customerId,
        List<OrderItem> items,
        ShippingMethod shippingMethod,
        BigDecimal totalAmount,
        Boolean highValue) {

    public static OrderEvent from(String orderId, Instant receivedAt, OrderRequest request) {
        return new OrderEvent(orderId, receivedAt, request.customerId(), List.copyOf(request.items()),
                request.shippingMethod(), null, null);
    }

    public OrderEvent withPricing(BigDecimal totalAmount, boolean highValue) {
        return new OrderEvent(orderId, receivedAt, customerId, items, shippingMethod, totalAmount, highValue);
    }
}
