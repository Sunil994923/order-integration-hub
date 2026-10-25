package io.github.sunil994923.orderhub.api;

import io.github.sunil994923.orderhub.model.OrderAccepted;
import io.github.sunil994923.orderhub.model.OrderRequest;
import io.github.sunil994923.orderhub.service.OrderSubmissionService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderSubmissionService submissionService;

    public OrderController(OrderSubmissionService submissionService) {
        this.submissionService = submissionService;
    }

    /** Accepts an order for asynchronous processing; returns 202 once it is on the inbound topic. */
    @PostMapping
    public ResponseEntity<OrderAccepted> submit(@Valid @RequestBody OrderRequest request) {
        String orderId = submissionService.submit(request);
        return ResponseEntity.accepted()
                .location(URI.create("/api/orders/" + orderId))
                .body(new OrderAccepted(orderId, "ACCEPTED"));
    }
}
