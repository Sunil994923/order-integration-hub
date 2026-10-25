package io.github.sunil994923.orderhub.service;

import io.github.sunil994923.orderhub.OrderHubProperties;
import io.github.sunil994923.orderhub.model.OrderEvent;
import io.github.sunil994923.orderhub.model.OrderItem;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** Calculates the order total and flags orders that need manual review. */
@Component
public class OrderEnricher {

    private final BigDecimal highValueThreshold;

    public OrderEnricher(OrderHubProperties properties) {
        this.highValueThreshold = properties.highValueThreshold();
    }

    public OrderEvent enrich(OrderEvent order) {
        if (order.items() == null || order.items().isEmpty()) {
            throw new InvalidOrderException("Order " + order.orderId() + " has no items");
        }
        BigDecimal total = order.items().stream()
                .map(OrderEnricher::lineTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(2, RoundingMode.HALF_UP);
        return order.withPricing(total, total.compareTo(highValueThreshold) >= 0);
    }

    private static BigDecimal lineTotal(OrderItem item) {
        if (item.unitPrice() == null || item.unitPrice().signum() <= 0 || item.quantity() <= 0) {
            throw new InvalidOrderException("Invalid line for SKU " + item.sku());
        }
        return item.unitPrice().multiply(BigDecimal.valueOf(item.quantity()));
    }
}
