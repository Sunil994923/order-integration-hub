package io.github.sunil994923.orderhub.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

/** Payload accepted by {@code POST /api/orders}. */
public record OrderRequest(
        @NotBlank @Size(max = 64)
        String customerId,
        @NotEmpty @Size(max = 100)
        List<@Valid OrderItem> items,
        @NotNull
        ShippingMethod shippingMethod) {
}
