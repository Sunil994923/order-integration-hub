package io.github.sunil994923.orderhub.model;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.math.BigDecimal;

public record OrderItem(
        @NotBlank @Pattern(regexp = "[A-Z0-9-]{3,32}", message = "must be 3-32 uppercase letters, digits or dashes")
        String sku,
        @Min(1) @Max(1000)
        int quantity,
        @NotNull @DecimalMin("0.01")
        BigDecimal unitPrice) {
}
